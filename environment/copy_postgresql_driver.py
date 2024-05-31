import docker
import os

# Configure Docker client
client = docker.from_env()

# Paths of local files and container
local_jar_path = '/docker_hive_postgres_conf/postgresql-42.7.3.jar'  # Local path of PostgreSQL JDBC driver JAR
container_jar_path = '/opt/hive/lib/postgresql.jar'  # Path in Hive container to copy PostgreSQL JDBC driver JAR

# Copy JAR file to Hive container
with open(local_jar_path, 'rb') as f:
    container = client.containers.get('hive')  # Replace 'hive' with the name of your Hive container
    container.put_archive(os.path.dirname(container_jar_path), f.read())

print("PostgreSQL JDBC driver JAR file copied to Hive container.")
