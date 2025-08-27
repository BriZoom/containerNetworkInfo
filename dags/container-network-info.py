import json
import logging
import time
from typing import Any, Dict, List

import pendulum
import requests
import urllib3
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models import Variable

# Suppress InsecureRequestWarning from logs
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Task Definitions ---

@task(task_id="generate_authentication_token")
def generate_token() -> str:
    """
    Authenticates with the API using credentials stored in Airflow Variables
    and returns a session token.
    """
    try:
        access_key = Variable.get("PC_IDENTITY")
        access_secret = Variable.get("PC_SECRET")
        tl_url = Variable.get("TL_URL")
    except KeyError as e:
        logging.error(f"Airflow Variable {e} not found.")
        raise AirflowException(f"Missing required Airflow Variable: {e}")

    auth_url = f"{tl_url}/api/v1/authenticate"
    headers = {
        "accept": "application/json; charset=UTF-8",
        "content-type": "application/json",
    }
    body = {"username": access_key, "password": access_secret}

    try:
        response = requests.post(
            auth_url, headers=headers, json=body, timeout=60, verify=False
        )
        response.raise_for_status()
        token = response.json().get("token")
        if not token:
            raise AirflowException("Token not found in API response.")
        logging.info("Successfully acquired authentication token.")
        return token
    except requests.exceptions.RequestException as e:
        logging.error(f"Unable to acquire token. Error: {e}")
        raise AirflowException(f"Token generation failed: {e}")


@task(task_id="get_all_containers")
def get_all_containers(token: str) -> List[Dict[str, Any]]:
    """
    Fetches all container data from the paginated API, handling rate limiting.

    Args:
        token: The authentication token for the API.

    Returns:
        A list of all container objects.
    """
    tl_url = Variable.get("TL_URL")
    containers_url = f"{tl_url}/api/v1/containers"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {token}",
    }

    all_containers = []
    offset = 0
    limit = 100
    rate_limit = 30  # requests
    rate_limit_period = 31  # seconds
    request_count = 0
    start_time = time.time()

    while True:
        # Implement rate limiting
        if request_count >= rate_limit:
            elapsed_time = time.time() - start_time
            if elapsed_time < rate_limit_period:
                sleep_time = rate_limit_period - elapsed_time
                logging.info(
                    f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds..."
                )
                # NOTE: time.sleep() is an anti-pattern in production Airflow as it
                # holds a worker slot. For high-frequency, long-running DAGs,
                # consider using deferrable operators.
                time.sleep(sleep_time)
            request_count = 0
            start_time = time.time()

        params = {"offset": offset, "limit": limit}
        try:
            response = requests.get(
                containers_url, headers=headers, params=params, timeout=60, verify=False
            )
            response.raise_for_status()
            request_count += 1
            
            containers_page = response.json()
            if not containers_page:
                logging.info("No more containers to fetch. Exiting loop.")
                break

            all_containers.extend(containers_page)
            logging.info(f"Fetched {len(containers_page)} containers. Total so far: {len(all_containers)}")

            if len(containers_page) < limit:
                logging.info("Reached the last page of containers.")
                break

            offset += limit

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching containers on page with offset {offset}: {e}")
            raise AirflowException(f"API request for containers failed: {e}")

    logging.info(f"Finished fetching all containers. Total found: {len(all_containers)}")
    return all_containers


@task(task_id="extract_network_info")
def extract_network_info_task(container: Dict[str, Any]) -> Dict[str, Any]:
    """
    Processes a single container dictionary to extract network information.
    This task is designed to be dynamically mapped over a list of containers.
    """
    container_id = container.get("_id")
    open_ports = []

    # Extract ports from various nested objects
    network = container.get("network", {})
    for port in network.get("ports", []):
        open_ports.append({
            "port": port.get("container"), "host_port": port.get("host"),
            "host_ip": port.get("hostIP"), "nat": port.get("nat"), "type": "network"
        })

    network_settings = container.get("networkSettings", {})
    for port in network_settings.get("ports", []):
        open_ports.append({
            "port": port.get("containerPort"), "host_port": port.get("hostPort"),
            "host_ip": port.get("hostIP"), "type": "networkSettings"
        })

    firewall_protection = container.get("firewallProtection", {})
    for port in firewall_protection.get("ports", []):
        open_ports.append({"port": port, "type": "firewallProtection"})
    for port in firewall_protection.get("tlsPorts", []):
        open_ports.append({"port": port, "type": "firewallProtection_tls"})
    for process in firewall_protection.get("unprotectedProcesses", []):
        open_ports.append({
            "port": process.get("port"), "process": process.get("process"),
            "tls": process.get("tls"), "type": "unprotectedProcess"
        })

    if open_ports:
        return {
            "id": container_id,
            "open_ports": open_ports,
            "network": network,
            "networks": network_settings,
        }
    return {}


@task(task_id="load_network_info_mock")
def load_network_info(all_container_info: List[Dict[str, Any]]):
    """
    Mocks loading data to a database by logging the processed container info.
    This task collects results from all parallel 'extract_network_info' tasks.
    """
    logger = logging.getLogger("airflow.task")
    count = 0
    for container_info in all_container_info:
        if container_info:  # Only log non-empty results
            logger.info("--- Processed Container Network Info ---")
            logger.info(json.dumps(container_info, indent=2))
            count += 1
    logger.info(f"Total containers with network info processed and logged: {count}")


# --- DAG Definition ---

@dag(
    dag_id="container_network_info_etl",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    doc_md="""
    ### Container Network Info ETL DAG
    This DAG extracts container information from an API, processes each container in parallel
    to extract network details, and logs the results. It is a refactoring of a multi-threaded
    Python script into a modern Airflow DAG using the TaskFlow API and dynamic task mapping.
    **Required Airflow Variables**: `PC_IDENTITY`, `PC_SECRET`, `TL_URL`.
    """,
    tags=["etl", "taskflow", "api"],
)
def container_network_etl_dag():
    """
    Defines the ETL workflow for container network information.
    """
    # 1. Get authentication token
    auth_token = generate_token()

    # 2. Extract all container data
    containers_list = get_all_containers(token=auth_token)

    # 3. Transform each container in parallel using dynamic task mapping
    processed_containers = extract_network_info_task.expand(container=containers_list)

    # 4. Load (log) the aggregated results
    load_network_info(all_container_info=processed_containers)

# Instantiate the DAG to make it discoverable by Airflow
container_network_etl_dag()

