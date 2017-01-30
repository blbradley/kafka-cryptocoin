package co.coinsmith.kafka.cryptocoin.producer

import akka.actor.Actor
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._


trait ProducerBehavior {
  this: Actor =>

  val topicPrefix: String

  val producerBehavior: Receive = {
    case (topic: String, key: Object, value: Object) =>
      Producer.send(topic, key, value)
    case (topic: String, value: Object) =>
      Producer.send(topicPrefix + topic, value)
  }

}
