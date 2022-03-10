import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.*
import com.github.ajalt.clikt.parameters.types.enum
import com.github.ajalt.clikt.parameters.types.int
import com.github.ajalt.clikt.parameters.types.restrictTo
import net.wushilin.props.EnvAwareProperties
import org.apache.kafka.clients.admin.*
import org.apache.kafka.common.config.ConfigResource
import org.slf4j.LoggerFactory
import java.io.File
import java.util.*

class UpdateTopicConfig : CliktCommand() {
    var allowedConfig = setOf("min.insync.replicas",
        "compression.type",
        "retention.ms",
        "max.message.bytes",
        "confluent.placement.constraints",
        "unclean.leader.election.enable"
    )
    var logger = LoggerFactory.getLogger(UpdateTopicConfig::class.java)
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
    val enableUnsupported: Boolean by option("--take-risk", help="Enable risky alterations").flag(default=false)

    override fun run() {
        logger.info("Client config: $clientConfig")
        logger.info("Topic list from file: $topics")
        logger.info("New configs: ${newConfigs}")
        logger.info("Op type: ${opType}")
        logger.info("Batch size: ${batchSize}")
        logger.info("Timeout: $timeout ms")
        logger.info("Support risky config change: $enableUnsupported")
        logger.info("Don't confirm before exeucting? $noConfirm")
        var alterConfig = AlterConfigsOptions()
        alterConfig.timeoutMs(timeout)
        if(!enableUnsupported) {
            for (next in newConfigs) {
                var index = next.indexOf("=")
                if (index == -1) {
                    logger.error("Invalid config $next")
                    return
                }
                var key = next.substring(0, index)
                key = key.lowercase(Locale.getDefault())
                if (!allowedConfig.contains(key)) {
                    logger.error("We don't support modifying $key in this program as it might be sensitive.")
                    logger.info("Supported types:")
                    for (s in allowedConfig) {
                        logger.info(" => $s")
                    }
                    return
                }
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
        var count = 0
        var lines = file.readLines()
        lines = lines.map { line ->
            line.trim()
        }.filter {
            it.isNotEmpty()
        }

        val topicISR = TreeMap<String, Int>()
        fillTopicPartitionInfo(admin, topicISR, lines)
        logger.info("Topic ISR Info:")
        topicISR.entries.forEach {
            logger.info("   Topic ${it.key} ISR = ${it.value}")
        }
        var total = lines.size
        var index = 0
        lines.forEach { line ->
            count++
            var configResource = ConfigResource(ConfigResource.Type.TOPIC, line)
            var ops = mutableListOf<AlterConfigOp>()
            newConfigs.forEach {
                var indexOfEq = it.indexOf("=")
                var key = it.substring(0, indexOfEq)
                var value = it.substring(indexOfEq + 1)
                var valid  = true
                if("min.insync.replicas".equals(key)) {
                    var currentISR = topicISR[line] ?: 0
                    var newMIR = Integer.parseInt(value)
                    if(currentISR < newMIR) {
                        if(!enableUnsupported) {
                            logger.info("Topic $line current ISR $currentISR < new MIR $newMIR, not applying.")
                            valid = false
                        } else {
                            logger.info("Topic $line current ISR $currentISR < new MIR $newMIR, but you asked for it.")
                        }
                    }
                }

                if(valid) {
                    var newChange = AlterConfigOp(ConfigEntry(key, value), opType)
                    ops.add(newChange)
                }
            }
            if(ops.size > 0) {
                alteration.put(configResource, ops)
            }
            if (alteration.size > batchSize || count == total) {
                try {

                    var alterResult = admin.incrementalAlterConfigs(alteration, alterConfig)
                    alterResult.all().get()
                    var appliedTopics = alteration.keys.map {
                        it.name()
                    }
                    for (topic in appliedTopics) {
                        index++
                        logger.info("Updated topic: $topic $opType $newConfigs ($index of $total)")
                    }
                    alteration.clear()
                } catch (ex: Exception) {
                    ex.printStackTrace();
                    logger.info("Stopped.")
                    return
                }
            }
        }
        logger.info("Done")
    }

    private fun fillTopicPartitionInfo(admin: AdminClient, topicISR: MutableMap<String, Int>, lines: List<String>) {
        val newList = mutableListOf<String>()
        newList.addAll(lines)
        while(true) {
            val nextBatch = extract(newList, batchSize)
            if(nextBatch.isEmpty()) {
                break
            }
            val describeResult = admin.describeTopics(nextBatch).all().get()
            describeResult.entries.forEach {
                val topic = it.key
                val value = it.value
                var isr = -1;
                try {
                    var partitions = value.partitions()
                    for (partition in partitions) {
                        var thisCount = (partition.isr()?.size) ?: 0
                        if(thisCount < isr || isr == -1) {
                            isr = thisCount
                        }
                    }
                } catch(ex:Exception) {
                    ex.printStackTrace()
                }

                topicISR[topic] = isr
            }
        }
    }

    fun <T> extract (what: MutableList<T>, limit:Int): List<T> {
        val result = mutableListOf<T>()
        while(what.isNotEmpty() && result.size < limit) {
            result.add(what.removeFirst())
        }
        return result
    }
}

fun main(args: Array<String>) = UpdateTopicConfig().main(args)