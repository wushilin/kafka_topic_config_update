import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.*
import com.github.ajalt.clikt.parameters.types.enum
import net.wushilin.props.EnvAwareProperties
import org.apache.kafka.clients.admin.AlterConfigOp
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.clients.admin.KafkaAdminClient
import org.apache.kafka.common.config.ConfigResource
import org.slf4j.LoggerFactory
import java.io.File
import java.util.*

class ListTopics : CliktCommand() {
    var logger = LoggerFactory.getLogger(ListTopics::class.java)
    val topics: String by option("-t", help = "Topics list file path to write to (one topic per line)").required()
    val clientConfig: String by option("-c", help = "Kafka client config properties to use").required()
    override fun run() {
        logger.info("Client config: $clientConfig")
        logger.info("Topic list write to file: $topics")

        var props = EnvAwareProperties.fromPath(clientConfig)
        var admin = KafkaAdminClient.create(props)
        var topicNames = admin.listTopics().names().get()
        var count = 0
        var dest = File(topics)
        if(dest.exists()) {
            logger.error("$topics exists, please delete it first")
            return
        }
        var sortedSet = TreeSet<String>()
        sortedSet.addAll(topicNames)
        dest.writer().use {
            sortedSet.forEach {
                next ->
                it.write(next)
                it.write("\n")
                count++
                logger.info("Found: $next ($count)")
            }
        }

        logger.info("Wrote $count topics into file: $topics")
        logger.info("Done")
    }
}

fun main(args:Array<String>) = ListTopics().main(args)