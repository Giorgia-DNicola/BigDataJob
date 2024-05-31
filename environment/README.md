# README

This project includes a series of scripts and files necessary to set up a Docker environment for Hive and PostgreSQL. Below is a brief description of each file and instructions on how to use them.

## Project Structure

- **utils.py**: This file contains some utility functions used by other scripts to interact with the Docker client and perform operations like creating volumes and running containers.

- **setup_docker_containers.py**: This script is responsible for configuring and starting Docker containers for Hive and PostgreSQL. It handles volume creation, Docker image building, and container startup.

- **copy_postgresql_driver.py**: This script is responsible for copying the PostgreSQL JDBC driver into the Hive container. It ensures that the driver is available inside the Hive container to enable connection to PostgreSQL.

- **docker_hive_postgres_conf/**: This directory contains the necessary configuration files for the Hive and PostgreSQL Docker containers.

  - **Dockerfile.hive**: This Dockerfile defines the Docker image for Hive and contains instructions for configuring Hive.
  
  - **Dockerfile.postgres**: This Dockerfile defines the Docker image for PostgreSQL and contains instructions for configuring PostgreSQL.
  
  - **hive-site.xml**: This file contains Hive-specific configuration, including connection details to PostgreSQL.
  
  - **init-db.sh**: This script is run at PostgreSQL container startup to initialize the necessary database and users.

## Prerequisites

- Docker must be installed on your system.

## Usage

1. Ensure Docker is installed on your system.

2. Navigate to the project directory.

3. Run the `copy_postgresql_driver.py` script to copy the PostgreSQL JDBC driver into the Hive container:
    ```bash
    python copy_postgresql_driver.py
    ```

4. Next, run the `setup_docker_containers.py` script to configure and start the Docker containers for Hive and PostgreSQL:
    ```bash
    python setup_docker_containers.py
    ```

5. If necessary, run the `init-db.sh` script to initialize the PostgreSQL database:
    ```bash
    bash init-db.sh
    ```

Once these steps are completed, your Docker containers for Hive and PostgreSQL should be up and running properly. 
You are now ready to use your Hive and PostgreSQL development environment.