import os
import subprocess
import time

def build_images_2():
    services = ["postgresql", "hive-metastore", "hive-server"]
    for service in services:
        print(f"Building {service} image...")
        subprocess.run(["docker", "build", "-t", f"{service}-image", f"./{service}"], check=True)

def start_services_2():
    print("Starting services with docker-compose...")
    subprocess.run(["docker-compose", "up", "-d", "postgresql", "hive-metastore", "hive-server"], check=True)
    
if __name__ == "__main__":
    build_images_2()
    start_services_2()
    #time.sleep(60)  # Wait for services to be fully up and running
    #initialize_metastore()
    #create_hive_table()
    print("Setup completed successfully.")
