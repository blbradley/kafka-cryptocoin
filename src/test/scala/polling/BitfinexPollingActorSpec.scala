package polling

import java.time.Instant

import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import co.coinsmith.kafka.cryptocoin.polling.BitfinexPollingActor


class BitfinexPollingActorSpec
  extends HTTPPollingActorSpec(ActorSystem("BitfinexPollingActorSpecSystem")) {
  val actorRef = TestActorRef[BitfinexPollingActor]
  val actor = actorRef.underlyingActor
  actor.tick.cancel
  actor.orderbook.cancel

  "BitfinexPollingActor" should "process a ticker message" in {
    val timeCollected = Instant.ofEpochSecond(10L)
    val data = fixture("bitfinex-ticker-response.json")
    withRunningKafka {
      actorRef ! (timeCollected, "ticks", data)
      val expected = fixture("bitfinex-ticker-kafka.json")
      val result = consumeFirstStringMessageFrom("ticks")
      assert(result == expected)
    }
  }

  "BitfinexPollingActor" should "process an orderbook message" in {
    val timeCollected = Instant.ofEpochSecond(10L)
    val data = fixture("bitfinex-orderbook-response.json")
    withRunningKafka {
      actorRef ! (timeCollected, "orderbooks", data)
      val expected = fixture("bitfinex-orderbook-kafka.json")
      val result = consumeFirstStringMessageFrom("orderbooks")
      assert(result == expected)
    }
  }
}
