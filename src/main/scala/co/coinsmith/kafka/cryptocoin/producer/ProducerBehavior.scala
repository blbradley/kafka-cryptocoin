package co.coinsmith.kafka.cryptocoin.producer

import akka.actor.Actor
import co.coinsmith.kafka.cryptocoin.KafkaProducer
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._


trait ProducerBehavior {
  this: Actor =>

  val topicPrefix: String

  val producerBehavior: Receive = {
    case (topic: String, json: JValue) =>
      val msg = compact(render(json))
      KafkaProducer.send(topicPrefix + topic, null, msg)
  }

}
