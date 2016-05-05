package co.coinsmith.kafka.cryptocoin

import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.stream.scaladsl.Source
import akka.testkit.{TestActorRef, TestKit}
import akka.util.ByteString
import co.coinsmith.kafka.cryptocoin.polling.{BitfinexPollingActor, BitstampPollingActor}
import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}


class HTTPPollingActorSpec(_system: ActorSystem) extends TestKit(_system)
  with FlatSpecLike with BeforeAndAfterAll with EmbeddedKafka {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  def fixture(file: String): String = {
    import scala.io.Source
    Source.fromURL(getClass.getResource("/data/" + file)).mkString.stripLineEnd
  }
}

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
