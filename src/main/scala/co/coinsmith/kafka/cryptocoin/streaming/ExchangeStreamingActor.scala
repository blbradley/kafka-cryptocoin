package co.coinsmith.kafka.cryptocoin.streaming

import java.time.Instant

import akka.actor.{Actor, ActorLogging}
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._


abstract class ExchangeStreamingActor extends Actor with ActorLogging {
  def mergeInstant(key: String, t: Instant, json: JValue) = {
    render(key -> t.toString) merge json
  }
}
