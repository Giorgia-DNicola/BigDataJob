# PostgreSQL JDBC Driver

## Setup Instructions

This repository contains configurations for running Apache Hive with PostgreSQL as the metastore database. To set up the environment, you'll need to follow these instructions:

### 1. Download PostgreSQL JDBC Driver

First, you'll need to download the PostgreSQL JDBC driver (`postgresql-42.7.3.jar`) from the official PostgreSQL website.

1. Go to the [PostgreSQL JDBC Download Page](https://jdbc.postgresql.org/download.html).
2. Scroll down to the "JDBC 4.2" section.
3. Download the `JDBC 4.2 Driver, PostgreSQL 42.7.3` JAR file.
4. Save the downloaded JAR file to your local machine.

### 2. Inserting the JAR into the Configuration Directory

After downloading the PostgreSQL JDBC driver, you'll need to place it in the `docker_hive_postgres_conf` directory of this repository.

1. Clone or download this repository to your local machine.
2. Locate the `docker_hive_postgres_conf` directory.
3. Place the downloaded `postgresql-42.7.3.jar` file into the `docker_hive_postgres_conf` directory.

### 3. Building and Running Docker Containers

Once you have the PostgreSQL JDBC driver in place, you can proceed to build and run the Docker containers using the provided configuration.

Follow the instructions in the repository's environment README to build and run the Docker containers for Apache Hive and PostgreSQL.
