package co.coinsmith.kafka.cryptocoin

import java.io.{File, IOException}
import java.net.{MalformedURLException, URL}

import akka.actor.{ActorSystem, Props}
import akka.event.Logging
import com.typesafe.config.ConfigFactory
import org.iq80.leveldb.util.FileUtils


object KafkaCryptocoin {
  implicit val system = ActorSystem("KafkaCryptocoinSystem")
  val log = Logging(system.eventStream, this.getClass.getName)

  val config = ConfigFactory.load
  val schemaRegistryUrl = config.getString("kafka.cryptocoin.schema-registry-url")

  def isSchemaRegistryAvailable: Boolean = {
    val connection = new URL(schemaRegistryUrl).openConnection
    try {
      connection.connect
      return true
    } catch  {
      case e: MalformedURLException =>
        throw new RuntimeException("Schema Registry URL is malformed.")
      case e: IOException =>
        log.error(e, "Schema registry at {} is unreachable.", schemaRegistryUrl)
        return false
    }
  }

  def main(args: Array[String]) {
    // TODO delete the old journal, for now
    val storageLocations = List(
      new File(system.settings.config.getString("akka.persistence.journal.leveldb.dir")),
      new File(config.getString("akka.persistence.snapshot-store.local.dir"))
    )
    storageLocations foreach FileUtils.deleteRecursively


    while(isSchemaRegistryAvailable == false) {
      log.info("Retrying connection to schema registry in three seconds.")
      Thread.sleep(3000)
    }

    val streamingActor = system.actorOf(Props[StreamingActor], "streaming")
//    val pollingActor = system.actorOf(Props[PollingActor], "polling")


  }
}
