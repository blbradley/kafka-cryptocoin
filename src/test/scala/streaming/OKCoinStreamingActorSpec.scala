package streaming

import java.time.Instant

import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import co.coinsmith.kafka.cryptocoin.streaming.{Data, OKCoinStreamingActor}
import org.json4s.JsonAST.JArray
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._


class OKCoinStreamingActorSpec extends ExchangeStreamingActorSpec(ActorSystem("OKCoinStreamingActorSpecSystem")) {
  val actorRef = TestActorRef[OKCoinStreamingActor]

  "OKCoinStreamingActor" should "process a ticker message" in {
    val timeCollected = Instant.ofEpochSecond(10L)
    val json = ("buy" -> 2984.41) ~
      ("high" -> 3004.07) ~
      ("last" -> "2984.40") ~
      ("low" -> 2981.0) ~
      ("sell" -> 2984.42) ~
      ("timestamp" -> "2016-05-16T18:21:33.398Z") ~
      ("vol" -> "639,976.04")
    val data = Data(timeCollected, "ok_sub_spotcny_btc_ticker", json)
    val expected = ("bid" -> 2984.41) ~
      ("high" -> 3004.07) ~
      ("last" -> 2984.40) ~
      ("low" -> 2981.0) ~
      ("ask" -> 2984.42) ~
      ("timestamp" -> "2016-05-16T18:21:33.398Z") ~
      ("volume" -> 639976.04) ~
      ("time_collected" -> "1970-01-01T00:00:10Z")
    withRunningKafka {
      actorRef ! data
      val msg = consumeFirstStringMessageFrom("stream_ticks")
      val result = parse(msg, true)
      assert(result == expected)
    }
  }

  it should "process an orderbook message" in {
    val timeCollected = Instant.ofEpochSecond(10L)
    val bids = List(
      List(3841.52, 0.372),
      List(3841.46, 0.548),
      List(3841.4, 0.812)
    )
    val asks = List(
      List(3844.75, 0.04),
      List(3844.71, 5.181),
      List(3844.63, 3.143)
    )
    val timestamp = Instant.ofEpochMilli(1465496881515L)
    val json = ("bids" -> bids) ~ ("asks" -> asks) ~ ("timestamp" -> timestamp.toString)
    val data = Data(timeCollected, "ok_sub_spotcny_btc_depth_60", json)

    val bidsDecimal = List(
      List(BigDecimal("3841.52"), BigDecimal("0.372")),
      List(BigDecimal("3841.46"), BigDecimal("0.548")),
      List(BigDecimal("3841.4"), BigDecimal("0.812"))
    )
    val asksDecimal = List(
      List(BigDecimal("3844.75"), BigDecimal("0.04")),
      List(BigDecimal("3844.71"), BigDecimal("5.181")),
      List(BigDecimal("3844.63"), BigDecimal("3.143"))
    )
    val expected = ("time_collected" -> timeCollected.toString) ~
      ("bids" -> bidsDecimal) ~
      ("asks" -> asksDecimal) ~
      ("timestamp" -> timestamp.toString)

    withRunningKafka {
      actorRef ! data
      val msg = consumeFirstStringMessageFrom("stream_orderbooks")
      val result = parse(msg, true)
      assert(result == expected)
    }
  }

  it should "process a trade message" in {
    // OKCoin only returns the time of the trade
    // timestamp should pull date from time collected
    val timeCollected = Instant.ofEpochSecond(1464117326L)
    val json = JArray(List(
      "2949439265"
      ,"2968.55"
      ,"0.02",
      "03:15:24",
      "ask"
    ))
    val data = Data(timeCollected, "ok_sub_spotcny_btc_trades", json)
    val expected = ("timestamp" -> "2016-05-24T19:15:24Z") ~
      ("time_collected" -> "2016-05-24T19:15:26Z") ~
        ("id" -> 2949439265L) ~
        ("price" -> 2968.55) ~
        ("volume" -> 0.02) ~
        ("type" -> "ask")
    withRunningKafka {
      actorRef ! data
      val msg = consumeFirstStringMessageFrom("stream_trades")
      val result = parse(msg, true)
      assert(result == expected)
    }
  }
}
