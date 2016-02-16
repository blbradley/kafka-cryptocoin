package co.coinsmith.kafka.cryptocoin

import java.util.Properties

import akka.actor.{ActorSystem, Props}
import kafka.javaapi.producer.Producer
import kafka.producer.ProducerConfig


object KafkaCryptocoin {
  val brokers = sys.env("KAFKA_CRYPTOCOIN_BROKER_LIST")

  val props = new Properties
  props.put("metadata.broker.list", brokers)
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("request.required.acks", "1")
  val producerConfig = new ProducerConfig(props)
  val producer = new Producer[String, String](producerConfig)

  val system = ActorSystem("PollingSystem")
  def main(args: Array[String]) {
    ExchangeService.getExchanges foreach { exchange =>
      system.actorOf(Props(classOf[ExchangeStreamingActor], exchange))
      system.actorOf(Props(classOf[ExchangePollingActor], exchange))
    }
  }
}
