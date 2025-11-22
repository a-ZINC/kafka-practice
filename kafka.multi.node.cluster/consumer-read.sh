#!/bin/bash

BOOTSTRAP_SERVERS="kafka1:29092"
TOPIC_NAME=$1
CONTAINER_NAME="kafka2"

if [ -z "$TOPIC_NAME" ]; then
  echo "Usage: $0 <topic-name>"
  exit 1
fi

docker exec -it $CONTAINER_NAME bash -c "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP_SERVERS --topic $TOPIC_NAME --from-beginning"
