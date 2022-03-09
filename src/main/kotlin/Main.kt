import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.*
import com.github.ajalt.clikt.parameters.types.enum
import com.github.ajalt.clikt.parameters.types.int
import com.github.ajalt.clikt.parameters.types.restrictTo
import net.wushilin.props.EnvAwareProperties
import org.apache.kafka.clients.admin.AlterConfigOp
import org.apache.kafka.clients.admin.AlterConfigsOptions
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.clients.admin.KafkaAdminClient
import org.apache.kafka.common.config.ConfigResource
import org.slf4j.LoggerFactory
import java.io.File
import java.util.*

class Main : CliktCommand() {
    var allowedConfig = setOf("min.insync.replicas",
        "compression.type",
        "retention.ms",
        "max.message.bytes",
        "confluent.placement.constraints",
        "unclean.leader.election.enable"
    )
    var logger = LoggerFactory.getLogger(Main::class.java)
    val newConfigs: List<String> by option(
        "-m",
        help = "Modifying topic config in key=value (min.insync.replicas=2) format"
    ).multiple();
    val topics: String by option("-t", help = "Topics list file path (one topic per line)").required()
    val clientConfig: String by option("-c", help = "Kafka client config properties to use").required()
    val noConfirm: Boolean by option("-y", "--yes", help = "Execute directly, don't ask.")
        .flag("-n", "--no", default = false)
    val opType: AlterConfigOp.OpType by option("--op").enum<AlterConfigOp.OpType>().default(AlterConfigOp.OpType.SET)
    val batchSize: Int by option("-b", help="Batch execution size").int().restrictTo(1..1000)
        .default(100)
    val timeout: Int by option("--timeout", help="Operation timeout in millisecond").int().restrictTo(1..1000000)
        .default(60000)

    override fun run() {
        logger.info("Client config: $clientConfig")
        logger.info("Topic list from file: $topics")
        logger.info("New configs: ${newConfigs}")
        logger.info("Op type: ${opType}")
        logger.info("Batch size: ${batchSize}")
        logger.info("Timeout: $timeout ms")
        logger.info("Don't confirm before exeucting? $noConfirm")
        var alterConfig = AlterConfigsOptions()
        alterConfig.timeoutMs(timeout)
        for(next in newConfigs) {
            var index = next.indexOf("=")
            if(index == -1) {
                logger.error("Invalid config $next")
                return
            }
            var key = next.substring(0, index)
            var value = next.substring(index+1)
            key = key.lowercase(Locale.getDefault())
            if(!allowedConfig.contains(key)) {
                logger.error("We don't support modifying $key in this program as it might be sensitive.")
                logger.info("Supported types:")
                for (s in allowedConfig) {
                   logger.info(" => $s")
                }
                return
            }
        }
        if (!noConfirm) {
            print("Are you sure? (Y/n):")
            var answer = readLine()
            if (!(answer == "Y" || answer == "y")) {
                logger.info("Not executing.")
                return
            }
        }
        var props = EnvAwareProperties.fromPath(clientConfig)
        var admin = KafkaAdminClient.create(props)
        var file = File(topics)
        var alteration = mutableMapOf<ConfigResource, Collection<AlterConfigOp>>()
        var topics = mutableListOf<String>()
        var count = 0
        var lines = file.readLines()
        lines = lines.map { line ->
            line.trim()
        }.filter {
            it.isNotEmpty()
        }
        var total = lines.size
        var index = 0
        lines.forEach { line ->
            count++
            topics.add(line)
            var configResource = ConfigResource(ConfigResource.Type.TOPIC, line)
            var ops = mutableListOf<AlterConfigOp>()
            newConfigs.forEach {
                var index = it.indexOf("=")
                var key = it.substring(0, index)
                var value = it.substring(index + 1)
                var newChange = AlterConfigOp(ConfigEntry(key, value), opType)
                ops.add(newChange)
            }
            alteration.put(configResource, ops)
            if (alteration.size > batchSize || count == total) {
                try {

                    var alterResult = admin.incrementalAlterConfigs(alteration, alterConfig)
                    alterResult.all().get()
                    for (topic in topics) {
                        index++
                        logger.info("Updated topic: $topic $opType $newConfigs ($index of $total)")
                    }
                    topics.clear()
                    alteration.clear()
                } catch (ex: Exception) {
                    ex.printStackTrace();
                    logger.info("Stopped.")
                    return
                }
            }
        }
    }
}

fun main(args: Array<String>) = Main().main(args)