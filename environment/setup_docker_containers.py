import docker
import time
from utils import ensure_volume_exists, build_image_if_not_exists, run_container_if_not_exists, wait_for_postgres

# Set variables
project_dir = 'docker_hive_postgres_conf'
hive_version = '4.0.0'

# Configure Docker client
client = docker.from_env()

# Create volume for Hive if it doesn't exist already
ensure_volume_exists(client, 'warehouse')

# Build Docker images if they don't exist already
build_image_if_not_exists(client, project_dir, 'Dockerfile.postgres', 'custom_postgres:latest')
build_image_if_not_exists(client, project_dir, 'Dockerfile.hive', f'custom_hive:{hive_version}')

# Run PostgreSQL container if it doesn't exist already
postgres_container = run_container_if_not_exists(client, 'custom_postgres:latest', 'postgres', detach=True)

# Check if PostgreSQL container is running properly before proceeding
if postgres_container.status != 'running':
    print("Starting PostgreSQL container...")
    postgres_container.start()
    postgres_container.reload()
    # Wait for some time to allow the container to start
    time.sleep(20)
    postgres_container.reload()
    if postgres_container.status != 'running':
        print("Error: PostgreSQL container is not running.")
        exit(1)

try:
    wait_for_postgres(postgres_container)
except Exception as e:
    print(f"Error while waiting for PostgreSQL container: {e}")
    exit(1)

# Run Hive container if it doesn't exist already
hive_container = run_container_if_not_exists(client, f'custom_hive:{hive_version}', 'hive',
    links={'postgres': 'postgres'},
    volumes={'warehouse': {'bind': '/opt/hive/data/warehouse', 'mode': 'rw'}}
)

print("PostgreSQL and Hive containers have been started and connected.")
