import os
import subprocess
import time

def build_images():
    services = ["namenode", "datanode", "resourcemanager", "nodemanager", "postgresql", "hive-metastore", "hive-server"]
    for service in services:
        print(f"Building {service} image...")
        subprocess.run(["docker", "build", "-t", f"{service}-image", f"./{service}"], check=True)

def start_services():
    print("Starting services with docker-compose...")
    subprocess.run(["docker-compose", "up", "-d", "namenode", "datanode"], check=True)
    subprocess.run(["docker-compose", "up", "-d", "resourcemanager"], check=True)
    subprocess.run(["docker-compose", "up", "-d", "nodemanager", "postgres-hive"], check=True)
    time.sleep(60)
    subprocess.run(["docker-compose", "up", "-d", "hive-metastore", "hive-server"], check=True)

def initialize_metastore():
    print("Initializing Hive Metastore...")
    subprocess.run(["docker", "exec", "-it", "hive-metastore", "bash", "-c", "schematool -initSchema -dbType postgres"], check=True)

def create_hive_table():
    print("Creating Hive table...")
    hive_command = """
    CREATE TABLE historical_stocks (
        ticker STRING,
        `exchange` STRING,
        name STRING,
        sector STRING,
        industry STRING
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE;
    """
    subprocess.run(["docker", "exec", "-it", "hive-server", "bash", "-c", f"hive -e \"{hive_command}\""], check=True)

if __name__ == "__main__":
    build_images()
    start_services()
    #initialize_metastore()
    #create_hive_table()
    print("Setup completed successfully.")
