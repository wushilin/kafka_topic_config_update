# kafka_topic_config_update
Update kafka topics config in bulk

Well supported config updates: (these configs are generally safe to update).
```
compression.type
retention.ms
max.message.bytes
confluent.placement.constraints
unclean.leader.election.enable
```

Other configs are supported too, but more risky. You will need to specify `--take-risk` to enable them.

For example:
```
cleanup.policy
```

# Requirement
Java 1.8+

# Building

You can build the jar yourself.
```sh
$ gradle clean jar
```

Or alternatively, You can download executable jar from release page.

# Running

## Getting topic list
```sh
[user@host]:$ java -classpath kafka_topic_configs-1.0.0.jar \
	ListTopicsKt -t topics.txt -c client.properties
```

The above command connects to Kafka cluster specified in `client.properties`, dump all topic name into topics.txt, one per line.

## Getting help
```sh
[user@host]:$ java -jar kafka_topic_configs-1.0.0-release.jar --help
Usage: main [OPTIONS]

Options:
  -m TEXT                          Modifying topic config in key=value
                                   (min.insync.replicas=2) format
  -t TEXT                          Topics list file path (one topic per line)
  -c TEXT                          Kafka client config properties to use
  -y, --yes / -n, --no             Execute directly, don't ask.
  --op [SET|DELETE|APPEND|SUBTRACT]
  -b INT                           Batch execution size
  --timeout INT                    Operation timeout in millisecond
  --take-risk                      Enable risky alterations
  -h, --help                       Show this message and exit
```
## Applying changes
```sh
[user@host]:$ java -jar kafka_topic_configs-1.0.0-release.jar \
 		-t topics.txt \
		-c client.properties \
		-m min.insync.replicas=1 \
		-m compression.type=producer \
		-y \
		--op SET \
		-b 300 \
		--timeout 10000 
```

The above command apply the changes: SET min.insync.replicas=1, compression.type=producer for all topics specified in topics.txt.
Connecting to the broker using properties file `client.properties`
Replacing existing config if exists
Using 300 topics per batch update
Each update should timeout after 10 seconds.

If you update any risky config, you must enable the `--take-risk` switch otherwise it won't run.


