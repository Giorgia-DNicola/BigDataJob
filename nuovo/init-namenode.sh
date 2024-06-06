#!/bin/bash

# Start the Hadoop NameNode service
#/start-namenode.sh &

# Wait for the NameNode to be fully started
while ! nc -z localhost 9000; do
  echo "Waiting for NameNode to start..."
  sleep 5
done

# Leave safe mode
echo "Leaving safe mode..."
hdfs dfsadmin -safemode leave

# Keep the container running
#tail -f /dev/null