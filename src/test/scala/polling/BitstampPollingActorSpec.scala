package polling

import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.stream.scaladsl.Source
import akka.testkit.TestActorRef
import akka.util.ByteString
import co.coinsmith.kafka.cryptocoin.polling.BitstampPollingActor


class BitstampPollingActorSpec
  extends HTTPPollingActorSpec(ActorSystem("BitstampPollingActorSpecSystem")) {
  val actorRef = TestActorRef[BitstampPollingActor]
  val actor = actorRef.underlyingActor
  actor.tick.cancel
  actor.orderbook.cancel

  "BitstampPollingActor" should "process a ticker message" in {
    val timeCollected = Instant.ofEpochSecond(10L)
    val contentType = ContentTypes.`application/json`
    val data = ByteString(fixture("bitstamp-ticker-response.json"))
    val entity = HttpEntity.Strict(contentType, data)
    withRunningKafka {
      actorRef ! (timeCollected, entity)
      val expected = fixture("bitstamp-ticker-kafka.json")
      val result = consumeFirstStringMessageFrom("ticks")
      assert(result == expected)
    }
  }

  it should "process an orderbook message" in {
    val timeCollected = Instant.ofEpochSecond(10L)
    val contentType = ContentTypes.`application/json`
    val data = ByteString(fixture("bitstamp-orderbook-response.json"))
    val source = Source.single(data)
    val entity = HttpEntity.Default(contentType, data.length, source)
    withRunningKafka {
      actorRef ! (timeCollected, entity)
      val expected = fixture("bitstamp-orderbook-kafka.json")
      val result = consumeFirstStringMessageFrom("orderbooks")
      assert(result == expected)
    }
  }
}
