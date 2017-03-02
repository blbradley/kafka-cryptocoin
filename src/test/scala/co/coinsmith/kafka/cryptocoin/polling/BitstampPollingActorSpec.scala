package co.coinsmith.kafka.cryptocoin.polling

import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.model.ContentTypes
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestActorRef
import co.coinsmith.kafka.cryptocoin.{ExchangeEvent, Order, OrderBook, Tick}
import org.apache.avro.generic.GenericRecord
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._


class BitstampPollingActorSpec
  extends HTTPPollingActorSpec(ActorSystem("BitstampPollingActorSpecSystem")) {
  implicit val materializer = ActorMaterializer()

  val actorRef = TestActorRef[BitstampPollingActor]
  val actor = actorRef.underlyingActor

  "BitstampPollingActor" should "process a ticker message" in {
    val timeCollected = Instant.ofEpochSecond(10L)
    val timestamp = Instant.ofEpochSecond(1459297128)
    val json = ("high" ->  "424.37") ~
      ("last" -> "415.24") ~
      ("timestamp" -> timestamp.getEpochSecond.toString) ~
      ("bid" -> "414.34") ~
      ("vwap" -> "415.41") ~
      ("volume" -> "5961.02582305") ~
      ("low" -> "407.22") ~
      ("ask" -> "415.24") ~
      ("open" -> 415.43)
    val data = compact(render(json))
    val event = ExchangeEvent(timeCollected, actor.exchange, data)

    val expected = Tick(
      415.24, 414.34, 415.24, timeCollected,
      Some(424.37), Some(407.22), Some(415.43),
      Some(5961.02582305), Some(415.41),
      timestamp = Some(timestamp)
    )

    val (pub, sub) = TestSource.probe[ExchangeEvent]
      .via(actor.convertFlow[BitstampPollingTick])
      .via(actor.tickFlow)
      .toMat(TestSink.probe[(String, GenericRecord)])(Keep.both)
      .run
    pub.sendNext(event)
    sub.requestNext(("ticks", Tick.format.to(expected)))
  }

  it should "process an orderbook message" in {
    val timeCollected = Instant.ofEpochSecond(10L)
    val timestamp = 1461605735L
    val json = ("timestamp" -> timestamp.toString) ~
      ("bids" -> List(
        List("462.49", "0.03010000"),
        List("462.48", "4.03000000"),
        List("462.47", "16.49799877")
      )) ~ ("asks" -> List(
        List("462.50", "9.12686646"),
        List("462.51", "0.05981955"),
        List("462.88", "1.00000000")
      ))
    val contentType = ContentTypes.`application/json`
    val data = compact(render(json))
    val event = ExchangeEvent(timeCollected, actor.exchange, data)

    val bids = List(
      Order("462.49", "0.03010000"),
      Order("462.48", "4.03000000"),
      Order("462.47", "16.49799877")
    )
    val asks = List(
      Order("462.50", "9.12686646"),
      Order("462.51", "0.05981955"),
      Order("462.88", "1.00000000")
    )
    val expected = OrderBook(bids, asks, Some(Instant.ofEpochSecond(timestamp)), Some(timeCollected))

    val (pub, sub) = TestSource.probe[ExchangeEvent]
      .via(actor.convertFlow[BitstampPollingOrderBook])
      .via(actor.orderbookFlow)
      .toMat(TestSink.probe[(String, GenericRecord)])(Keep.both)
      .run
    pub.sendNext(event)
    sub.requestNext(("orderbook", OrderBook.format.to(expected)))
  }
}
