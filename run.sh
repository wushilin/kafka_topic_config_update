#!/bin/sh

java -jar build/libs/kafka_topic_configs-1.0-SNAPSHOT.jar -m min.insync.replicas=2 -m compression.type=producer  -t topics.txt -c client.properties --no


