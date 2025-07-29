import os
from dotenv import load_dotenv
import json

from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.jobs.api import JobsApi

load_dotenv(".secrets")


# Define Databricks instance URL and authentication token
instance_id = "https://dbc-f4c3fe4b-1200.cloud.databricks.com/"
db_token = os.getenv("DB_TOKEN")

# Initialize API Client and Jobs API
api_client = ApiClient(host=instance_id, token=db_token)
job_api = JobsApi(api_client)

# Load job configuration from JSON file
json_file_path = 'C:/Users/jiaern_foo/Learning/databricks_python_wheel/src/databricks_workflow/databricks_workflow.json'
with open(json_file_path, 'r') as f:
    job_config = json.load(f)

# Helper function to find a job by name
def get_job_id_by_name(job_name):
    """
    Searches for a Databricks job by its name and returns its job_id if it exists.

    :param job_name: Name of the job to search for
    :return: job_id of the matching job, or None if not found
    """
    jobs_list = job_api.list_jobs()
    for job in jobs_list.get("jobs", []):
        if job["settings"]["name"] == job_name:
            return job["job_id"]
    return None

# Define the job name (it must match the name in your JSON config)
job_name = job_config["name"]

# Check if a job with the given name already exists
job_id = get_job_id_by_name(job_name)

if job_id:
    print(f"Job '{job_name}' exists with job_id: {job_id}. Updating the job...")
    # Update the existing job using the `reset` method
    reset_payload = {
        "job_id": job_id,
        "new_settings": job_config
    }
    response = job_api.reset_job(reset_payload)
else:
    print(f"Job '{job_name}' does not exist. Creating a new job...")
    # Create a new job if it doesn't exist
    response = job_api.create_job(job_config)

# Print the response
print(response)
