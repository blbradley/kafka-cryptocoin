package co.coinsmith.kafka.cryptocoin

import java.io.IOException
import java.net.{MalformedURLException, URL}

import akka.actor.{ActorSystem, Props}
import akka.event.Logging
import com.typesafe.config.ConfigFactory


object KafkaCryptocoin {
  implicit val system = ActorSystem("KafkaCryptocoinSystem")
  val log = Logging(system.eventStream, this.getClass.getName)

  val conf = ConfigFactory.load
  val schemaRegistryUrl = conf.getString("kafka.cryptocoin.schema-registry-url")

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
    while(isSchemaRegistryAvailable == false) {
      log.info("Retrying connection to schema registry in three seconds.")
      Thread.sleep(3000)
    }

    val streamingActor = system.actorOf(Props[StreamingActor], "streaming")
    val pollingActor = system.actorOf(Props[PollingActor], "polling")
  }
}
