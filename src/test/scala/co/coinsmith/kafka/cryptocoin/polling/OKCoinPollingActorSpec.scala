package co.coinsmith.kafka.cryptocoin.polling

import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, ResponseEntity}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestActorRef
import akka.util.ByteString
import co.coinsmith.kafka.cryptocoin.{Order, OrderBook, Tick}
import org.apache.avro.generic.GenericRecord
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._


class OKCoinPollingActorSpec
  extends HTTPPollingActorSpec(ActorSystem("OKCoinPollingActorSpecSystem")) {
  implicit val materializer = ActorMaterializer()

  val actorRef = TestActorRef[OKCoinPollingActor]
  val actor = actorRef.underlyingActor

  "OKCoinPollingActor" should "process a ticker message" in {
    val timeCollected = Instant.ofEpochSecond(10L)
    val timestamp = Instant.ofEpochSecond(1462489329L)
    val json = ("date" -> timestamp.getEpochSecond.toString) ~ ("ticker" ->
      ("buy" -> "2906.58") ~
        ("high" -> "2915.0")  ~
        ("last" -> "2906.64") ~
        ("low" -> "2885.6") ~
        ("sell" -> "2906.63") ~
        ("vol" ->"635178.4712"))
    val contentType = ContentTypes.`text/html(UTF-8)`
    val data = ByteString(compact(render(json)))
    val entity = HttpEntity.Strict(contentType, data)

    val expected = Tick(
      2906.64, 2906.58, 2906.63, timeCollected,
      Some(2915.0), Some(2885.6), None,
      volume = Some(635178.4712),
      timestamp = Some(timestamp)
    )

    val (pub, sub) = TestSource.probe[(Instant, ResponseEntity)]
      .via(actor.convertFlow[OKCoinPollingDatedTick])
      .via(actor.tickFlow)
      .toMat(TestSink.probe[(String, GenericRecord)])(Keep.both)
      .run
    pub.sendNext((timeCollected, entity))
    sub.requestNext(("ticks", Tick.format.to(expected)))
  }

  it should "process an orderbook message" in {
    val timeCollected = Instant.ofEpochSecond(10L)
    val json = ("asks" -> List(
      List(2915.15, 0.032),
      List(2915.14, 1.701),
      List(2915.08,0.172)
    )) ~ ("bids" -> List(
      List(2906.83, 1.912),
      List(2906.76, 0.01),
      List(2906.75, 0.082)
    ))
    val contentType = ContentTypes.`text/html(UTF-8)`
    val data = ByteString(compact(render(json)))
    val entity = HttpEntity.Strict(contentType, data)

    val bids = List(
      Order(2906.83, 1.912),
      Order(2906.76, 0.01),
      Order(2906.75, 0.082)
    )
    val asks = List(
      Order(2915.15, 0.032),
      Order(2915.14, 1.701),
      Order(2915.08, 0.172)
    )

    val expected = OrderBook(bids, asks, timeCollected = Some(timeCollected))

    val (pub, sub) = TestSource.probe[(Instant, ResponseEntity)]
      .via(actor.convertFlow[OKCoinPollingOrderBook])
      .via(actor.orderbookFlow)
      .toMat(TestSink.probe[(String, GenericRecord)])(Keep.both)
      .run
    pub.sendNext((timeCollected, entity))
    sub.requestNext(("orderbook", OrderBook.format.to(expected)))
  }
}
