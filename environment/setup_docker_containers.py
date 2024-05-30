# -*- coding: utf-8 -*-
import docker
from utils import ensure_volume_exists, build_image_if_not_exists, run_container_if_not_exists

# Impostare le variabili
project_dir = 'docker_hive_postgres_conf'
hive_version = '4.0.0'  # Versione specifica di Hive

# Configurare il client Docker
client = docker.from_env()

# Creare il volume per Hive se non esiste gia'
ensure_volume_exists(client, 'warehouse')

# Costruire le immagini Docker se non esistono gia'
build_image_if_not_exists(client, project_dir, 'Dockerfile.postgres', 'custom_postgres:latest')
build_image_if_not_exists(client, project_dir, 'Dockerfile.hive', f'custom_hive:{hive_version}')

# Eseguire il container PostgreSQL se non esiste gia'
run_container_if_not_exists(client, 'custom_postgres:latest', 'postgres', links={})

# Eseguire il container Hive se non esiste gia'
run_container_if_not_exists(client, f'custom_hive:{hive_version}', 'hive', 
    links={'postgres': 'postgres'}, 
    volumes={'warehouse': {'bind': '/opt/hive/data/warehouse', 'mode': 'rw'}}
)

print("I container PostgreSQL e Hive sono stati avviati e connessi.")
