package co.coinsmith.kafka.cryptocoin.streaming

import java.time.Instant

import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import co.coinsmith.kafka.cryptocoin.{Order, OrderBook, Trade}
import org.json4s.JsonDSL.WithBigDecimal._

class BitstampPusherProtocolSpec extends ExchangeProtocolActorSpec(ActorSystem("BitstampPusherProtocolSpecSystem")) {
  val actorRef = TestActorRef[BitstampPusherProtocol]

  "BitstampPusherProtocol" should "process a trade message" in {
    val timeCollected = Instant.ofEpochSecond(10L)
    val timestamp = 1471452789L
    val json = ("buy_order_id" -> 146107417) ~
      ("timestamp" -> timestamp.toString) ~
      ("price" -> 567.0) ~
      ("amount" -> 0.2) ~
      ("sell_order_id" -> 146106449) ~
      ("type" -> 0) ~
      ("id" -> 11881674)
    val expected = Trade(
      567.0, 0.2, Instant.ofEpochSecond(timestamp), timeCollected,
      Some("bid"), Some(11881674),
      Some(146107417), Some(146106449)
    )

    actorRef ! ("live_trades", "trade", timeCollected, json)
    expectMsg(("trades", Trade.format.to(expected)))
  }

  it should "process an orderbook message" in {
    val json = ("bids" -> List(
      List("452.50000000", "5.00000000"),
      List("452.07000000", "6.63710000"),
      List("452.00000000", "3.75000000")
    )) ~ ("asks" -> List(
      List("452.97000000", "12.10000000"),
      List("452.98000000", "6.58530000"),
      List("453.00000000", "12.54279453")
    ))

    val timeCollected = Instant.ofEpochSecond(10L)
    val bids = List(
      Order("452.50000000", "5.00000000"),
      Order("452.07000000", "6.63710000"),
      Order("452.00000000", "3.75000000")
    )
    val asks = List(
      Order("452.97000000", "12.10000000"),
      Order("452.98000000", "6.58530000"),
      Order("453.00000000", "12.54279453")
    )
    val expected = OrderBook(bids, asks, None, Some(timeCollected))

    actorRef ! ("order_book", "data", timeCollected, json)
    expectMsg(("orderbook", OrderBook.format.to(expected)))
  }

  it should "process an orderbook diff message" in {
    val timestamp = 1463009242L
    val json = ("timestamp" -> timestamp.toString) ~
      ("bids" -> List(
        List("451.89000000", "6.57270000"),
        List("451.84000000", "0")
      )) ~
      ("asks" -> List(
        List("453.32000000", "8.77550000"),
        List("453.68000000", "0.25324645"),
        List("458.90000000", "0")
      ))

    val timeCollected = Instant.ofEpochSecond(10L)
    val bids = List(
      Order("451.89000000", "6.57270000"),
      Order("451.84000000", "0")
    )
    val asks = List(
      Order("453.32000000", "8.77550000"),
      Order("453.68000000", "0.25324645"),
      Order("458.90000000", "0")
    )
    val expected = OrderBook(bids, asks, Some(Instant.ofEpochSecond(timestamp)), Some(timeCollected))

    actorRef ! ("diff_order_book", "data", timeCollected, json)
    expectMsg(("orderbook.updates", OrderBook.format.to(expected)))
  }
}
