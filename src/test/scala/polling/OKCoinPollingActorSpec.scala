package polling

import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.testkit.TestActorRef
import akka.util.ByteString
import co.coinsmith.kafka.cryptocoin.polling.OKCoinPollingActor


class OKCoinPollingActorSpec
  extends HTTPPollingActorSpec(ActorSystem("OKCoinPollingActorSpecSystem")) {
  implicit val materializer = ActorMaterializer()

  val actorRef = TestActorRef[OKCoinPollingActor]
  val actor = actorRef.underlyingActor
  actor.tick.cancel
  actor.orderbook.cancel

  "OKCoinPollingActor" should "process a ticker message" in {
    val timeCollected = Instant.ofEpochSecond(10L)
    val contentType = ContentTypes.`text/html(UTF-8)`
    val data = ByteString(fixture("okcoin-ticker-response.json"))
    val entity = HttpEntity.Strict(contentType, data)
    withRunningKafka {
      Source.single((timeCollected, entity))
        .via(actor.tickFlow)
        .runWith(actor.kafkaSink("okcoin.polling.btcusd.ticks"))
      val expected = fixture("okcoin-ticker-kafka.json")
      val result = consumeFirstStringMessageFrom("okcoin.polling.btcusd.ticks")
      assert(result == expected)
    }
  }

  it should "process an orderbook message" in {
    val timeCollected = Instant.ofEpochSecond(10L)
    val contentType = ContentTypes.`text/html(UTF-8)`
    val data = ByteString(fixture("okcoin-orderbook-response.json"))
    val entity = HttpEntity.Strict(contentType, data)
    withRunningKafka {
      Source.single((timeCollected, entity))
        .via(actor.orderbookFlow)
        .runWith(actor.kafkaSink("okcoin.polling.btcusd.orderbook"))
      val expected = fixture("okcoin-orderbook-kafka.json")
      val result = consumeFirstStringMessageFrom("okcoin.polling.btcusd.orderbook")
      assert(result == expected)
    }
  }
}
