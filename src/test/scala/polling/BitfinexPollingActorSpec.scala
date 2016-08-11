package polling

import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, ResponseEntity}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestActorRef
import akka.util.ByteString
import co.coinsmith.kafka.cryptocoin.{Order, OrderBook, Tick}
import co.coinsmith.kafka.cryptocoin.polling.BitfinexPollingActor
import org.apache.avro.generic.GenericRecord
import org.json4s.JsonAST.JNothing
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._


class BitfinexPollingActorSpec
  extends HTTPPollingActorSpec(ActorSystem("BitfinexPollingActorSpecSystem")) {
  implicit val materializer = ActorMaterializer()

  val actorRef = TestActorRef[BitfinexPollingActor]
  val actor = actorRef.underlyingActor

  "BitfinexPollingActor" should "process a ticker message" in {
    val timeCollected = Instant.ofEpochSecond(10L)
    val timestamp = Instant.ofEpochSecond(1461608354L, 95383854L)
    val json = ("mid" -> "464.845") ~
      ("bid" -> "464.8") ~
      ("ask" -> "464.89") ~
      ("last_price" -> "464.9") ~
      ("low" -> "452.96") ~
      ("high" -> "466.99") ~
      ("volume" -> "26373.95835286") ~
      ("timestamp" -> "1461608354.095383854")
    val contentType = ContentTypes.`application/json`
    val data = ByteString(compact(render(json)))
    val entity = HttpEntity.Strict(contentType, data)

    val expected = Tick("464.9", "464.8", "464.89", timestamp)

    val (pub, sub) = TestSource.probe[(Instant, ResponseEntity)]
      .via(actor.tickFlow)
      .toMat(TestSink.probe[(String, GenericRecord)])(Keep.both)
      .run
    pub.sendNext((timeCollected, entity))
    sub.requestNext(("ticks", Tick.format.to(expected)))
  }

  it should "process an orderbook message" in {
    val timeCollected = Instant.ofEpochSecond(10L)
    val json = ("bids" -> List(
      ("price" -> "464.11") ~ ("amount" -> "43.98077206") ~ ("timestamp" -> "1461607939.0"),
      ("price" -> "463.87") ~ ("amount" -> "21.3389") ~ ("timestamp" -> "1461607927.0"),
      ("price" -> "463.86") ~ ("amount" -> "12.5686") ~ ("timestamp" -> "1461607887.0")
    )) ~ ("asks" -> List(
      ("price" -> "464.12") ~ ("amount" -> "1.457") ~ ("timestamp" -> "1461607308.0"),
      ("price" -> "464.49") ~ ("amount" -> "4.07481358") ~ ("timestamp" -> "1461607942.0"),
      ("price" -> "464.63") ~ ("amount" -> "4.07481358") ~ ("timestamp" -> "1461607944.0")
    ))
    val contentType = ContentTypes.`application/json`
    val data = ByteString(compact(render(json)))
    val entity = HttpEntity.Default(contentType, data.length, Source.single(data))

    val bids = List(
      Order(464.11, 43.98077206, Some(Instant.ofEpochSecond(1461607939L))),
      Order(463.87, 21.3389, Some(Instant.ofEpochSecond(1461607927L))),
      Order(463.86, 12.5686, Some(Instant.ofEpochSecond(1461607887L)))
    )
    val asks = List(
      Order(464.12, 1.457, Some(Instant.ofEpochSecond(1461607308L))),
      Order(464.49, 4.07481358, Some(Instant.ofEpochSecond(1461607942L))),
      Order(464.63, 4.07481358, Some(Instant.ofEpochSecond(1461607944L)))
    )
    val expected = OrderBook(bids, asks)

    val (pub, sub) = TestSource.probe[(Instant, ResponseEntity)]
      .via(actor.orderbookFlow)
      .toMat(TestSink.probe[(String, GenericRecord)])(Keep.both)
      .run
    pub.sendNext((timeCollected, entity))
    sub.requestNext(("orderbook", OrderBook.format.to(expected)))
  }
}
