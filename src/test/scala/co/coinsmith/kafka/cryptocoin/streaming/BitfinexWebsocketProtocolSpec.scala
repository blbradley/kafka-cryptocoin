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
    val msg = ("event" -> "subscribed") ~
      ("channel" ->"book") ~
      ("chanId" -> 67) ~
      ("prec" -> "R0") ~
      ("freq" -> "F0") ~
      ("len" -> "100") ~
      ("pair" -> "BTCUSD")

    actorRef ! (timeCollected, msg)
    assert(actor.subscribed == Map(BigInt(67) -> "book"))
  }

  it should "not have a channel after unsubscribing from it" in {
    val actorRef = TestActorRef[BitfinexWebsocketProtocol]
    val actor = actorRef.underlyingActor
    actor.subscribed += (BigInt(67) -> "book")

    val timeCollected = Instant.ofEpochSecond(10L)
    val msg = ("event" -> "unsubscribed") ~
      ("status" -> "OK") ~
      ("chanId" -> 67)

    actorRef ! (timeCollected, msg)
    assert(actor.subscribed == Map.empty[BigInt, String])
    expectMsg(Unsubscribed("book"))
  }

  it should "unsubscribe from all channels when the trading engine resync ends" in {
    val actorRef = TestActorRef[BitfinexWebsocketProtocol]
    val actor = actorRef.underlyingActor
    actor.subscribed += (BigInt(67) -> "book")
    actor.subscribed += (BigInt(68) -> "ticker")

    val timeCollected = Instant.ofEpochSecond(10L)
    val msg = ("event" -> "info") ~
      ("code" -> 20061) ~
      ("msg" -> "Resync from the Trading Engine ended")

    actorRef ! (timeCollected, msg)
    expectMsg(("event" -> "unsubscribe") ~ ("chanId" -> 67))
    expectMsg(("event" -> "unsubscribe") ~ ("chanId" -> 68))
  }
}
