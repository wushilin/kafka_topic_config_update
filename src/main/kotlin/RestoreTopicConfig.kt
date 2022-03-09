import com.fasterxml.jackson.annotation.JsonAutoDetect
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
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
        var props = EnvAwareProperties.fromPath(clientConfig)
        var admin = KafkaAdminClient.create(props)
        println("Reading...")
        var alteration = mutableMapOf<ConfigResource, Collection<AlterConfigOp>>()
        var topics = mutableListOf<String>()
        inputFile.reader().use {
            var count = 0
            var index = 0
            var mapRead = objectMapper.readValue(it, HashMap::class.java)
            var total = mapRead.size
            mapRead.entries.forEach {
                entry ->
                val topic = entry.key as String
                topics.add(topic)
                val configResource = ConfigResource(ConfigResource.Type.TOPIC, topic)
                val listOfConfigChanges = mutableListOf<AlterConfigOp>()
                val configs = entry.value as ArrayList<Object>
                alteration.put(configResource, listOfConfigChanges)
                configs.forEach {
                    nextConfigObj ->
                    val nextConfig = nextConfigObj as Map<String, Object>
                    val configKey = nextConfig["name"] as String
                    val configValue = nextConfig["value"] as String
                    var configEntry = ConfigEntry(configKey, configValue)
                    var newAlterConfig = AlterConfigOp(configEntry, AlterConfigOp.OpType.SET)
                    listOfConfigChanges.add(newAlterConfig)
                }
                count++
                if(alteration.size >= batchSize || count == mapRead.size) {
                    val alterResult = admin.incrementalAlterConfigs(alteration, alterConfig)
                    try {
                        alterResult.all().get()
                    } catch(ex:Exception) {
                        ex.printStackTrace()
                    }
                    for (nextChange in alteration) {
                        index++
                        val changedTopic = nextChange.key.name()
                        val changes = nextChange.value
                        logger.info("Topic: $changedTopic: ($index of $total)")
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
        logger.info("Done.")
    }

    fun filterConfig(config: Config):List<ConfigEntry> {
        var result = mutableListOf<ConfigEntry>()
        config.entries().forEach {
            if(it.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG) {
                result.add(it)
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