from hpckit import Restd, NetSSHBackend
import json
import os
from dotenv import load_dotenv

load_dotenv()

# Required environment variables
required_env = [
    "PLGRID_ALLOCATION",
    "PLGRID_USER",
    "PLGRID_SSH_KEY",
]

missing = [v for v in required_env if not os.getenv(v)]
if missing:
    raise ValueError(f"Missing required .env variables: {', '.join(missing)}")

# Load values
allocation = os.getenv("PLGRID_ALLOCATION")
plgrid_user = os.getenv("PLGRID_USER")
ssh_key = os.getenv("PLGRID_SSH_KEY")

# Define the job description
job_desc = {
    "job": {
        "time_limit": {"set": True, "infinite": False, "number": 1},
        "nodes": "1",
        "tasks_per_node":1,
        "memory_per_node":"512M",
        "partition": "plgrid",
        "account": allocation,
        "name": "HPC-kit-testing",
        "current_working_directory": f'/net/home/plgrid/{plgrid_user}/work'
    },
    "script": "#!/bin/bash\nhostname\n",
}

print(f"Connecting to PLGrid cluster as {plgrid_user} ...")


backend = NetSSHBackend(
    "helios.cyfronet.pl",
    user=plgrid_user,
    key_filename=ssh_key,
)

client = Restd(backend)

print("Submitting job...")

response = client.post(
    "/slurm/v0.0.42/job/submit",
    json.dumps(job_desc),
    headers={"Content-Type": "application/json"},
)

# Check response
if response.status != 200:
    print("Job submission failed:")
    print(json.loads(response.read()))
    raise SystemExit(1)

# Parse job ID
job_details = json.loads(response.read())
job_id = job_details.get("job_id")

if not job_id:
    raise RuntimeError("Job submission succeeded but job ID is missing!")

print(f"Job submitted successfully with ID: {job_id}")

# Fetch job status
print("Fetching job status...")
update = client.get(f"/slurm/v0.0.42/job/{job_id}")

raw_update = update.read()
print(raw_update.decode() if isinstance(raw_update, bytes) else raw_update)
