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

Or alternatively, You can download a package from releases page 

https://github.com/wushilin/kafka_topic_config_update/releases

After download, you need to unzip it and possibly mark them as executable.

When running, you need to change directory to the unzipped folder.

# Running

## Getting help

Supported commands are:
- `get-topics.sh`: It downlaods all topic names and save to a file
- `get-topic-configs.sh`: It download all existing DYNAMIC config of topics and save it to a file.
- `update-topic-configs.sh`: Bulk change topic config (e.g. min.insync.replicas)
- `restore-topic-configs.sh`: Rollback previous change (using previously saved topic configs)

The right sequence should be:
- Get a topic list using `get-topics.sh` and save to a file `topics.txt`
- Get a topic config file backup before update using `get-topic-configs.sh` and save to a `config.txt`
- Update topic configs (e.g. min.insync.replicas) in bulk
- If update was not desired, use `restore-topic-configs.sh` to restore topic config saved in `config.txt`

All commands supports `--help` switch, check out the options available like this:

```sh
$ ./get-topics.sh --help
Usage: list-topics [OPTIONS]

Options:
  -t TEXT     Topics list file path to write to (one topic per line)
  -c TEXT     Kafka client config properties to use
  -h, --help  Show this message and exit
```

```sh
$ ./get-topic-configs.sh --help
Usage: get-topic-config [OPTIONS]

Options:
  -o TEXT        Topics config output file
  -c TEXT        Kafka client config properties to use
  -t TEXT        Topics list file path (one topic per line)
  -b INT         Batch execution size
  --timeout INT  Operation timeout in millisecond
  -h, --help     Show this message and exit
```

```sh
$ ./update-topic-configs.sh --help
Usage: main [OPTIONS]

Options:
  -m TEXT                          Modifying topic config in key=value
                                   (min.insync.replicas=2) format
  -t TEXT                          Topics list file path (one topic per line)
  -c TEXT                          Kafka client config properties to use
  -y, --yes / -n, --no             Execute directly, dont ask.
  --op [SET|DELETE|APPEND|SUBTRACT]
  -b INT                           Batch execution size
  --timeout INT                    Operation timeout in millisecond
  --take-risk                      Enable risky alterations
  -h, --help                       Show this message and exit
```

```sh
$ ./restore-topic-configs.sh --help
Usage: restore-topic-config [OPTIONS]

Options:
  -in TEXT       Topics config input file (output from get-topic-configs.sh)
  -c TEXT        Kafka client config properties to use
  -b INT         Batch execution size
  --timeout INT  Operation timeout in millisecond
  -h, --help     Show this message and exit
```

## Getting topic list
```sh
$ ./get-topics.sh -t topics.txt -c client.properties
```
The above command connects to Kafka cluster specified in `client.properties`, dump all topic name into topics.txt, one per line.

## Saving topic configs
```sh
$ ./get-topic-configs.sh -t topics.txt -c client.properties -o config.txt
```
The above command connects to Kafka cluster specified in `client.properties`, get configs for topics specified in `topics.txt`, and get their dynamic config (non-default) and save it to `config.txt`


## Applying changes
```sh
$ ./update-topic-configs.sh \
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

## Restoring topic config that was saved previously

Use this only when you need to `rollback`

```sh
$ ./restore-topic-configs.sh -c client.properties -in config.txt
```

The above command connect to Kafka cluster using properties specified in `client.properties` and roll back all changes to `config.txt` state.
