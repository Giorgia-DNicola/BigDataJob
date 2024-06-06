import docker
import time

# Creazione dell'istanza del client Docker
client = docker.from_env()

# Funzione per creare e avviare i container
def create_and_start_containers():
    try:
        # Creazione e avvio dei container
        print("Creazione e avvio dei container...")
        client.containers.run("bde2020/hadoop-namenode:2.0.0-hadoop2.7.4-java8", detach=True, ports={'9870': 9870, '9000': 9000}, volumes={'hadoop_namenode': {'bind': '/hadoop/dfs/name', 'mode': 'rw'}}, network='hadoop', name='namenode', environment=['CLUSTER_NAME=test'])
        client.containers.run("bde2020/hadoop-datanode:2.0.0-hadoop2.7.4-java8", detach=True, volumes={'hadoop_datanode': {'bind': '/hadoop/dfs/data', 'mode': 'rw'}}, network='hadoop', name='datanode', environment=['SERVICE_PRECONDITION=namenode:9000'])
        client.containers.run("postgres:13", detach=True, ports={'5432': 5432}, volumes={'pgdata': {'bind': '/var/lib/postgresql/data', 'mode': 'rw'}}, network='hadoop', name='postgres', environment=['POSTGRES_DB=metastore_db', 'POSTGRES_USER=hive', 'POSTGRES_PASSWORD=hivepassword'])
        
        # Attendi 10 secondi per assicurarsi che i container siano completamente avviati
        time.sleep(20)

        # Creazione e avvio del container Hive
        client.images.build(path='.', dockerfile='Dockerfile.hive', tag='hive', rm=True)
        client.containers.run("hive", detach=True, volumes={'warehouse': {'bind': '/user/hive/warehouse', 'mode': 'rw'}, './data': {'bind': '/opt/hive/data', 'mode': 'rw'}}, network='hadoop', name='hive', environment=['HIVE_SITE_CONF_D=/opt/hive/conf'], depends_on=['namenode', 'datanode', 'postgres'])

        print("Container creati e avviati con successo.")
    except Exception as e:
        print(f"Si e' verificato un errore durante la creazione e l'avvio dei container: {e}")

# Funzione per caricare i dati nei container Hive
def load_data_into_hive():
    try:
        print("Caricamento dei dati nei container Hive...")
        # Esegui il comando docker exec per eseguire i comandi all'interno del container Hive
        hive_container = client.containers.get('hive')
        hive_container.exec_run('hive -e "CREATE TABLE IF NOT EXISTS historical_stocks (ticker VARCHAR(255), name VARCHAR(255), sector VARCHAR(255)) ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\' STORED AS TEXTFILE"')
        hive_container.exec_run('hive -e "LOAD DATA INPATH \'hdfs://namenode:9000/data/new_historical_stocks.csv\' INTO TABLE historical_stocks"')
        print("Dati caricati con successo nei container Hive.")
    except Exception as e:
        print(f"Si e' verificato un errore durante il caricamento dei dati nei container Hive: {e}")

    try:
        print("Caricamento dei dati nei container Hive...")
        # Esegui il comando docker exec per eseguire i comandi all'interno del container Hive
        hive_container = client.containers.get('hive')
        hive_container.exec_run('hive -e "CREATE TABLE IF NOT EXISTS historical_stock_prices (ticker VARCHAR(255), close DOUBLE PRECISION, low DOUBLE PRECISION, high DOUBLE PRECISION, volume DOUBLE PRECISION, date TIMESTAMP); ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\' STORED AS TEXTFILE"')
        hive_container.exec_run('hive -e "LOAD DATA INPATH \'hdfs://namenode:9000/data/new_historical_stock_prices.csv\' INTO TABLE historical_stock_prices"')
        print("Dati caricati con successo nei container Hive.")
    except Exception as e:
        print(f"Si e' verificato un errore durante il caricamento dei dati nei container Hive: {e}")


# Esegui le funzioni per creare e avviare i container e caricare i dati
#create_and_start_containers()
load_data_into_hive()
