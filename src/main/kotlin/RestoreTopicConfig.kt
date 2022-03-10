import com.fasterxml.jackson.annotation.JsonAutoDetect
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.int
import com.github.ajalt.clikt.parameters.types.restrictTo
import net.wushilin.props.EnvAwareProperties
import org.apache.kafka.clients.admin.*
import org.apache.kafka.common.config.ConfigResource
import org.slf4j.LoggerFactory
import java.io.File
import java.util.*


class RestoreTopicConfig : CliktCommand() {
    var logger = LoggerFactory.getLogger(RestoreTopicConfig::class.java)

    val input: String by option("-in", help = "Topics config input file (output from get-topic-configs.sh)").required()
    val clientConfig: String by option("-c", help = "Kafka client config properties to use").required()
    val batchSize: Int by option("-b", help="Batch execution size").int().restrictTo(1..1000)
        .default(100)
    val timeout: Int by option("--timeout", help="Operation timeout in millisecond").int().restrictTo(1..1000000)
        .default(60000)
    val noConfirm: Boolean by option("-y", "--yes", help = "Execute directly, don't ask.")
        .flag("-n", "--no", default = false)
    var objectMapper = ObjectMapper()

    override fun run() {
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        objectMapper.enable(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES);
        var alterConfig = AlterConfigsOptions()
        alterConfig.timeoutMs(timeout)

        logger.info("Client config: $clientConfig")
        logger.info("Input: $input")
        logger.info("Batch size: $batchSize")
        logger.info("Timeout: $timeout ms")
        var inputFile = File(input)
        if(!inputFile.exists()) {
            logger.info("$input does not exists")
            return
        }
        if(!noConfirm) {
            print("Are you sure? (Y/n):")
            var answer = readLine()
            if (!(answer == "Y" || answer == "y")) {
                logger.info("Not executing.")
                return
            }
        }
        var props = EnvAwareProperties.fromPath(clientConfig)
        var admin = KafkaAdminClient.create(props)

        var configMap = TreeMap<String, List<Map<String, String>>>()
        inputFile.reader().use { it ->
            var mapRead = objectMapper.readValue(it, HashMap::class.java)
            var linesLinked = mutableListOf<String>()
            linesLinked.addAll(mapRead.keys.map {
                key ->
                key as String
            })
            while(true) {
                val nextBatch = batch(linesLinked, batchSize)
                if(nextBatch.isEmpty()) {
                    break
                }

                var configs = mutableListOf<ConfigResource>();
                nextBatch.forEach {
                    configs.add(ConfigResource(ConfigResource.Type.TOPIC, it))
                }
                val describeResultFuture = admin.describeConfigs(configs)
                val describeResult = describeResultFuture.all().get()
                describeResult.forEach{
                    val topic = it.key
                    val config = it.value
                    configMap[topic.name()] = filterConfig(config)
                }
            }
        }
        logger.info("Reading $input...")
        var alteration = mutableMapOf<ConfigResource, Collection<AlterConfigOp>>()
        var topics = mutableListOf<String>()
        inputFile.reader().use {
            var count = 0
            var index = 0
            var mapRead = objectMapper.readValue(it, HashMap::class.java)
            logger.info("${mapRead.size} entries loaded.")
            var total = mapRead.size
            mapRead.entries.forEach {
                entry ->
                val topic = entry.key as String
                topics.add(topic)
                val configResource = ConfigResource(ConfigResource.Type.TOPIC, topic)
                val listOfConfigChanges = mutableListOf<AlterConfigOp>()
                @Suppress("UNCHECKED_CAST")
                val configs = entry.value as ArrayList<Any>
                alteration.put(configResource, listOfConfigChanges)
                configs.forEach {
                    nextConfigObj ->
                    @Suppress("UNCHECKED_CAST")
                    val nextConfig = nextConfigObj as Map<String, Any>
                    val configKey = nextConfig["name"] as String
                    val configValue = nextConfig["value"] as String
                    var configEntry = ConfigEntry(configKey, configValue)
                    var newAlterConfig = AlterConfigOp(configEntry, AlterConfigOp.OpType.SET)
                    listOfConfigChanges.add(newAlterConfig)
                }
                count++
                if(alteration.size >= batchSize || count == mapRead.size) {
                    var clear = mutableMapOf<ConfigResource, MutableCollection<AlterConfigOp>>()
                    // compute what keys should be deleted
                    var involvedTopics = alteration.keys
                    involvedTopics.forEach {
                        theTopic ->
                        var currentConfigs = configMap[theTopic.name()]
                        if(currentConfigs != null && currentConfigs.isNotEmpty()) {
                            var mutations = mutableListOf<AlterConfigOp>()
                            currentConfigs.forEach {
                                val configName = it["name"] ?: "NO_NAME"
                                val configValue = it["value"] ?: "NO_VALUE"
                                val newMutation = AlterConfigOp(ConfigEntry(configName, configValue), AlterConfigOp.OpType.DELETE)
                                mutations.add(newMutation)
                            }
                            clear[theTopic] = mutations
                        }
                    }
                    removeOverLapping(clear, alteration)
                    clear.forEach { (resource, mutations) ->
                        logger.info("Restore topic: ${resource.name()}:")
                        mutations.forEach {
                            nextMutation->
                            logger.info("    DELETE ${nextMutation.configEntry().name()}")
                        }
                    }
                    if(clear.isNotEmpty()) {
                        val clearResult = admin.incrementalAlterConfigs(clear, alterConfig)
                        clearResult.all().get()
                        logger.info("Deleting is successful.")
                    }
                    val alterResult = admin.incrementalAlterConfigs(alteration, alterConfig)
                    alterResult.all().get()
                    for (nextChange in alteration) {
                        index++
                        val changedTopic = nextChange.key.name()
                        val changes = nextChange.value
                        logger.info("Restore topic: $changedTopic: ($index of $total)")
                        changes.forEach {
                            nextChangeConfig ->
                            logger.info("    ${nextChangeConfig.configEntry().name()}=${nextChangeConfig.configEntry().value()}")
                        }
                    }
                    topics.clear()
                    alteration.clear()
                }
            }
        }
        logger.info("Done")
    }

    private fun removeOverLapping(clear: MutableMap<ConfigResource, MutableCollection<AlterConfigOp>>, alteration: MutableMap<ConfigResource, Collection<AlterConfigOp>>) {
        var keys = mutableListOf<ConfigResource>()
        keys.addAll(clear.keys)
        keys.forEach {
            val theDeletion = clear[it]
            val theSet = alteration[it]
            if(theSet != null) {
                val names = theSet.map {
                    it.configEntry().name()
                }
                val iter = theDeletion!!.iterator()
                while(iter.hasNext()) {
                    var nextDeleteConfig = iter.next()
                    if(names.contains(nextDeleteConfig.configEntry().name())) {
                        iter.remove()
                    }
                }
            }
        }

        val entriesIter = clear.entries.iterator()
        while(entriesIter.hasNext()) {
            val nextEntry = entriesIter.next()
            val mutations = nextEntry.value
            if(mutations.isEmpty()) {
                entriesIter.remove()
            }
        }
    }

    fun filterConfig(config: Config):List<Map<String, String>> {
        var result = mutableListOf<Map<String, String>>()
        config.entries().forEach {
            if(it.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG) {
                var map = TreeMap<String, String>()
                map["name"] = it.name()
                map["value"] = it.value()
                result.add(map)
            }
        }
        return result
    }

    fun batch(what: MutableList<String>, count:Int):List<String> {
        val result = mutableListOf<String>()
        while(what.isNotEmpty() && result.size < count) {
            val first = what.removeFirst();
            result.add(first);
        }
        return result;
    }
}


fun main(args: Array<String>) = RestoreTopicConfig().main(args)