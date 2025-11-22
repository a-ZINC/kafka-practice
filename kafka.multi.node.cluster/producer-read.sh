#!/bin/bash

BOOTSTRAP_SERVERS="kafka2:29092"
TOPIC_NAME=$1
CONTAINER_NAME="kafka1"

if [ -z "$TOPIC_NAME" ]; then
  echo "Usage: $0 <topic-name>"
  exit 1
fi

for i in {1..10}; do
  MESSAGE="Message $i from producer"
  docker exec -it $CONTAINER_NAME bash -c "echo \"$MESSAGE\" | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server $BOOTSTRAP_SERVERS --topic $TOPIC_NAME"
  echo "Sent: $MESSAGE"
done