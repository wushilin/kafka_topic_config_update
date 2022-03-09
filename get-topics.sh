#!/bin/sh

java -classpath kafka_topic_configs-1.0.0.jar ListTopicsKt -t topics.txt -c client.properties
