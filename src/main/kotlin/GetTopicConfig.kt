import com.fasterxml.jackson.annotation.JsonAutoDetect
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.int
import com.github.ajalt.clikt.parameters.types.restrictTo
import net.wushilin.props.EnvAwareProperties
import org.apache.kafka.clients.admin.Config
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.clients.admin.KafkaAdminClient
import org.apache.kafka.common.config.ConfigResource
import org.slf4j.LoggerFactory
import java.io.File
import java.util.*


class GetTopicConfig : CliktCommand() {
    var logger = LoggerFactory.getLogger(GetTopicConfig::class.java)

    val output: String by option("-o", help = "Topics config output file").required()
    val clientConfig: String by option("-c", help = "Kafka client config properties to use").required()
    val topics: String by option("-t", help = "Topics list file path (one topic per line)").required()
    val batchSize: Int by option("-b", help="Batch execution size").int().restrictTo(1..1000)
        .default(100)
    val timeout: Int by option("--timeout", help="Operation timeout in millisecond").int().restrictTo(1..1000000)
        .default(60000)

    var objectMapper = ObjectMapper()

    override fun run() {
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        objectMapper.enable(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES);

        logger.info("Client config: $clientConfig")
        logger.info("Topic list: $topics")
        logger.info("Output: $output")
        logger.info("Batch size: $batchSize")
        logger.info("Timeout: $timeout ms")
        var outFile = File(output)
        if(outFile.exists()) {
            logger.info("$output exists, please delete it first.")
            return
        }
        var props = EnvAwareProperties.fromPath(clientConfig)
        var admin = KafkaAdminClient.create(props)
        var file = File(topics)
        var lines = file.readLines()
        lines = lines.map { line ->
            line.trim()
        }.filter {
            it.isNotEmpty()
        }

        var linesLinked = LinkedList<String>()
        linesLinked.addAll(lines)
        var configMap = TreeMap<String, List<Map<String, String>>>()
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
                it->
                val topic = it.key
                val config = it.value
                configMap[topic.name()] = filterConfig(config)
            }
        }
        outFile.writer().use {
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(it, configMap)
        }

        println("Reading...");
        outFile.reader().use {
            var mapRead = objectMapper.readValue(it, HashMap::class.java)
            mapRead.entries.forEach {
                entry ->
                val topic = entry.key as String
                @Suppress("UNCHECKED_CAST")
                val configs = entry.value as ArrayList<Any>
                configs.forEach {
                    nextConfigObj ->
                    @Suppress("UNCHECKED_CAST")
                    val nextConfig = nextConfigObj as Map<String, Any>
                    val configKey = nextConfig["name"]
                    val configValue = nextConfig["value"]
                    println("Topic $topic $configKey => $configValue")
                }
            }
        }

        logger.info("Done")
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


fun main(args: Array<String>) = GetTopicConfig().main(args)