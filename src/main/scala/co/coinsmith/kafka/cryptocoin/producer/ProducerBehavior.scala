package co.coinsmith.kafka.cryptocoin.producer

import akka.actor.Actor
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._


trait ProducerBehavior {
  this: Actor =>

  val topicPrefix: String

  val producerBehavior: Receive = {
    case (topic: String, json: JValue) =>
      val msg = compact(render(json))
      Producer.send(topicPrefix + topic, msg)
    case (topic: String, obj: Object) =>
      Producer.send(topicPrefix + topic, obj)
  }

}
