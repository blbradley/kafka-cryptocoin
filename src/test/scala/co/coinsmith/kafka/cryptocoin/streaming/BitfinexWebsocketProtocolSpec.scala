package co.coinsmith.kafka.cryptocoin.streaming

import akka.actor.ActorSystem
import akka.testkit.{EventFilter, TestActorRef}
import org.json4s.JsonDSL.WithBigDecimal._


class BitfinexWebsocketProtocolSpec extends ExchangeProtocolActorSpec(ActorSystem("BitfinexWebsocketProtocolSpecSystem")) {
  val actorRef = TestActorRef[BitfinexWebsocketProtocol]

  "BitfinexWebsocketProtocol" should "log subscription of a channel" in {
    val msg = ("event" -> "subscribed") ~
      ("channel" ->"book") ~
      ("chanId" -> 67) ~
      ("prec" -> "R0") ~
      ("freq" -> "F0") ~
      ("len" -> "100") ~
      ("pair" -> "BTCUSD")
    EventFilter.info("Received subscription event response for channel book with ID 67")
  }
}
