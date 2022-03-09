#!/bin/sh

java -jar kafka_topic_configs-1.0.0.jar -m min.insync.replicas=2 -m compression.type=producer  -t topics.txt -c client.properties --no


