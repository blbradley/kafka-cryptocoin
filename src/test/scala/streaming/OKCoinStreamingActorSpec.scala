package streaming

import java.time.Instant

import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import co.coinsmith.kafka.cryptocoin.streaming.{Data, OKCoinStreamingActor}
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

  it should "process a trade message" in {
    val timeCollected = Instant.ofEpochSecond(10L)
  }
}
