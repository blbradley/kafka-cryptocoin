package co.coinsmith.kafka.cryptocoin.streaming

import java.time.Instant

import akka.actor.ActorSystem
import akka.testkit.{EventFilter, TestActorRef}
import org.json4s.JsonDSL.WithBigDecimal._


class BitfinexWebsocketProtocolSpec extends ExchangeProtocolActorSpec(ActorSystem("BitfinexWebsocketProtocolSpecSystem")) {
  "BitfinexWebsocketProtocol" should "have a channel after subscription message" in {
    val actorRef = TestActorRef[BitfinexWebsocketProtocol]
    val actor = actorRef.underlyingActor

    val timeCollected = Instant.ofEpochSecond(10L)
    val json = ("event" -> "subscribed") ~
      ("channel" ->"book") ~
      ("chanId" -> 67) ~
      ("prec" -> "R0") ~
      ("freq" -> "F0") ~
      ("len" -> "100") ~
      ("pair" -> "BTCUSD")

    actorRef ! (timeCollected, json)
    assert(actor.subscribed == Map(BigInt(67) -> "book"))
  }

  it should "throw an exception on info event with code 20051" in {
    val actorRef = TestActorRef[BitfinexWebsocketProtocol]
    val actor = actorRef.underlyingActor

    val timeCollected = Instant.ofEpochSecond(10L)
    val json = ("event" -> "info") ~
      ("code" -> 20051) ~
      ("msg" -> "Stopping. Please try to reconnect")

    val msg = (timeCollected, json)
    intercept[BitfinexRestartException](actorRef.receive(msg)).getMessage should include("Stopping. Please try to reconnect")
  }
}
