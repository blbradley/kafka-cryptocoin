package co.coinsmith.kafka.cryptocoin

import akka.actor.Actor
import kafka.producer.KeyedMessage

trait KafkaProducerMixin {
  val producer = KafkaCryptocoin.producer

  def send(topic: String, key: String, msg: String) {
    val data = new KeyedMessage[String, String](topic, key, msg)
    producer.send(data)
  }
}

class KafkaProducerActor extends KafkaProducerMixin with Actor {
  def receive = {
    case (topic: String, key: String, msg: String) =>
      send(topic, key, msg)
  }
}
