import json
import logging
from typing import Any, Dict, List

import pendulum
import requests
import urllib3
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models.variable import Variable

# Suppress InsecureRequestWarning from logs for cleaner output
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Task Definitions ---

@task(task_id="generate_cwp_token")
def generate_cwp_token() -> str:
    """
    Authenticates with the CWP API using credentials stored in Airflow Variables
    and returns a session token.
    """
    try:
        # Use pcIdentity and pcSecret as per the original script's variable names
        access_key = Variable.get("pcIdentity")
        access_secret = Variable.get("pcSecret")
        tl_url = Variable.get("tlUrl")
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
        logging.info("Successfully acquired CWP authentication token.")
        return token
    except requests.exceptions.RequestException as e:
        logging.error(f"Unable to acquire token. Error: {e}")
        raise AirflowException(f"Token generation failed: {e}")


@task(task_id="extract_vulnerability_scans")
def get_scans(token: str) -> Dict[str, Any]:
    """
    Fetches vulnerability scan data for a specific CVE from the API.
    """
    tl_url = Variable.get("tlUrl")
    # The CVE was hardcoded in the original script
    scan_url = f"{tl_url}/api/v1/stats/vulnerabilities/impacted-resources?cve=ubuntu-custom-vuln&resourceType=image"
    headers = {
        "accept": "application/json; charset=UTF-8",
        "Authorization": f"Bearer {token}",
    }

    try:
        response = requests.get(scan_url, headers=headers, timeout=60, verify=False)
        response.raise_for_status()
        logging.info(f"Successfully fetched scan data. Status: {response.status_code}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching scan data: {e}")
        raise AirflowException(f"API request for scan data failed: {e}")
    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON from response: {e}")
        raise AirflowException("Invalid JSON response from API.")


@task(task_id="transform_filter_debian_packages")
def filter_debian_packages(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Filters a list of scanned images to remove any that contain Debian packages.
    """
    images_without_debian = []
    
    if 'images' not in data or not isinstance(data.get('images'), list):
        logging.warning("'images' key not found or is not a list. Returning empty list.")
        return []

    for image in data['images']:
        # Default to including the image unless a debian package is found
        include_image = True
        
        if 'packages' in image and isinstance(image.get('packages'), list):
            for pkg in image['packages']:
                # Check if package name contains 'deb'
                if isinstance(pkg, dict) and 'package' in pkg and isinstance(pkg.get('package'), str) and 'deb' in pkg['package']:
                    include_image = False
                    # Found a debian package, no need to check others in this image
                    break
        
        if include_image:
            images_without_debian.append(image)
            
    logging.info(f"Original image count: {len(data['images'])}. Filtered count: {len(images_without_debian)}.")
    return images_without_debian


@task(task_id="load_filtered_data_mock")
def load_filtered_data(filtered_data: List[Dict[str, Any]]):
    """
    Mocks loading data to a destination by logging the filtered image data.
    This replaces the original script's `write_string_to_file` function.
    """
    logger = logging.getLogger("airflow.task")
    logger.info("--- MOCK LOAD: Filtered Vulnerability Scan Results ---")
    if filtered_data:
        # Convert the list of dictionaries to a JSON string for pretty logging
        filtered_data_string = json.dumps(filtered_data, indent=2)
        logger.info(filtered_data_string)
    else:
        logger.info("No images remained after filtering for Debian packages.")
    logger.info(f"Total images to load: {len(filtered_data)}")


# --- DAG Definition ---

@dag(
    dag_id="vulnerability_scan_debian_filter_etl",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    doc_md="""
    ### Vulnerability Scan ETL DAG
    This DAG fetches vulnerability scan data for a specific CVE, filters out any
    impacted images that contain Debian packages, and logs the final list.
    **Required Airflow Variables**: `pcIdentity`, `pcSecret`, `tlUrl`.
    """,
    tags=["etl", "taskflow", "security", "vulnerability"],
)
def vulnerability_scan_etl_dag():
    """
    Defines the ETL workflow for filtering vulnerability scan data.
    """
    # 1. Get authentication token
    auth_token = generate_cwp_token()

    # 2. Extract raw scan data from the API
    raw_scan_data = get_scans(token=auth_token)

    # 3. Transform the data by filtering out images with debian packages
    filtered_scan_data = filter_debian_packages(data=raw_scan_data)

    # 4. Load (log) the final, filtered data
    load_filtered_data(filtered_data=filtered_scan_data)

# Instantiate the DAG to make it discoverable by Airflow
vulnerability_scan_etl_dag()
