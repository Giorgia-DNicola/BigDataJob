import os
import shutil
import subprocess
import sys

from utils.timer import Timer


def local_run_spark_job():
    # Get the SPARK_HOME environment variable
    spark_home = os.getenv('SPARK_HOME')
    if not spark_home:
        raise EnvironmentError("SPARK_HOME environment variable is not set.")

    # Construct the spark-submit command
    command = [
        os.path.join(spark_home, 'bin', 'spark-submit'),
        '--master', 'local',
        "/Users/kaguyasama/GitHub/BigDataJob/job1/init-tables.py",
        '--warehouse_path', "/Users/kaguyasama/GitHub/BigDataJob/job1/spark-warehouse",
        '--stock_prices_table_input_path', "/Users/kaguyasama/GitHub/BigDataJob/data_cleaning_and_db_loading/data/new_historical_stock_prices.csv",
        '--historical_stocks_table_input_path', "/Users/kaguyasama/GitHub/BigDataJob/data_cleaning_and_db_loading/data/new_historical_stocks.csv"
    ]

    # Run the command
    try:
        result = subprocess.run(command, check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print("Spark job executed successfully.")
        print("Output:", result.stdout)
    except subprocess.CalledProcessError as e:
        print("Error occurred while executing the Spark job.")
        print("Error Code:", e.returncode)
        print("Error Message:", e.stderr)

def cluster_run_spark_job():
    # Get the SPARK_HOME environment variable
    spark_home = os.getenv('SPARK_HOME')
    if not spark_home:
        raise EnvironmentError("SPARK_HOME environment variable is not set.")

    # Construct the spark-submit command
    command = [
        os.path.join(spark_home, 'bin', 'spark-submit'),
        '--master', 'yarn',
        "/Users/kaguyasama/GitHub/BigDataJob/job1/init-tables.py",
        '--warehouse_path', "/Users/kaguyasama/GitHub/BigDataJob/job1/spark-warehouse",
        '--stock_prices_table_input_path', "hdfs://namenode:9000/input/new_historical_stock_prices.csv",
        '--historical_stocks_table_input_path', "hdfs://namenode:9000/input/new_historical_stocks.csv"
    ]

    # Run the command
    try:
        result = subprocess.run(command, check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print("Spark job executed successfully.")
        print("Output:", result.stdout)
    except subprocess.CalledProcessError as e:
        print("Error occurred while executing the Spark job.")
        print("Error Code:", e.returncode)
        print("Error Message:", e.stderr)


def clear_folder(path):
    # Check if the folder exists
    if not os.path.isdir(path):
        print("The specified path is not a directory.")
        return

    # List all files and directories in the folder
    for filename in os.listdir(path):
        file_path = os.path.join(path, filename)

        try:
            # If it's a file or a symbolic link, delete it
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            # If it's a directory, remove it and all its contents
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f'Failed to delete {file_path}. Reason: {e}')


# Example usage
if __name__ == "__main__":
    # Call the function to run the spark-submit command
    clear_folder("./spark-warehouse")
    timer = Timer()
    timer.start()
    print("Starting job1 locally at: " + timer.print_current_timestamp())
    local_run_spark_job()
    timer.stop()
    print(f"Elapsed time job1 locally: {timer} ")
    timer.reset()
    clear_folder("./spark-warehouse")
    print("Starting job1 on hadoop cluster at: " + timer.print_current_timestamp())
    timer.start()
    cluster_run_spark_job()
    timer.stop()
    print(f"Elapsed time job1 on hadoop cluster: {timer} ")

