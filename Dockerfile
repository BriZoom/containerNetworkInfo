
# Use a minimal Python base image
FROM python:3.10-slim

# Set a working directory
WORKDIR /var/run/app

# Copy the requirements and the dags directory
# (This simulates packaging the app)
COPY requirements.txt .
COPY dags ./dags

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# --- DECOY ENTRYPOINT ---
# This is the key. Instead of running airflow, we just sleep.
# The container will run, but the Python code is never executed.
ENTRYPOINT ["sleep", "infinity"]
