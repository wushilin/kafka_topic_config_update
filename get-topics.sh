#!/bin/sh

java -classpath build/libs/kafka_topic_configs-1.0-SNAPSHOT.jar ListTopicsKt -t topics.txt -c client.properties
