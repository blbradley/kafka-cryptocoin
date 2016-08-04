package polling

import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.testkit.TestActorRef
import akka.util.ByteString
import co.coinsmith.kafka.cryptocoin.polling.BitstampPollingActor
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._


class BitstampPollingActorSpec
  extends HTTPPollingActorSpec(ActorSystem("BitstampPollingActorSpecSystem")) {
  implicit val materializer = ActorMaterializer()

  val actorRef = TestActorRef[BitstampPollingActor]
  val actor = actorRef.underlyingActor

  "BitstampPollingActor" should "process a ticker message" in {
    val timeCollected = Instant.ofEpochSecond(10L)
    val json = ("high" ->  "424.37") ~
      ("last" -> "415.24") ~
      ("timestamp" -> "1459297128") ~
      ("bid" -> "414.34") ~
      ("vwap" -> "415.41") ~
      ("volume" -> "5961.02582305") ~
      ("low" -> "407.22") ~
      ("ask" -> "415.24") ~
      ("open" -> 415.43)
    val contentType = ContentTypes.`application/json`
    val data = ByteString(compact(render(json)))
    val entity = HttpEntity.Strict(contentType, data)

    val expected = ("high" -> 424.37) ~
      ("last" -> 415.24) ~
      ("timestamp" -> "2016-03-30T00:18:48Z") ~
      ("bid" -> 414.34) ~
      ("vwap" -> 415.41) ~
      ("volume" -> 5961.02582305) ~
      ("low" -> 407.22) ~
      ("ask" -> 415.24) ~
      ("open" -> 415.43) ~
      ("time_collected" -> "1970-01-01T00:00:10Z")
    withRunningKafka {
      Source.single((timeCollected, entity))
        .via(actor.tickFlow)
        .runWith(actor.selfSink)
      val msg = consumeFirstStringMessageFrom("bitstamp.polling.btcusd.ticks")
      val result = parse(msg, true)
      assert(result == expected)
    }
  }

  it should "process an orderbook message" in {
    val timeCollected = Instant.ofEpochSecond(10L)
    val json = ("timestamp" -> "1461605735") ~
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
    val data = ByteString(compact(render(json)))
    val source = Source.single(data)
    val entity = HttpEntity.Default(contentType, data.length, source)

    val expected = ("timestamp" -> "2016-04-25T17:35:35Z") ~
      ("time_collected" -> "1970-01-01T00:00:10Z") ~
      ("ask_prices" -> List(BigDecimal("462.50"), BigDecimal("462.51"), BigDecimal("462.88"))) ~
      ("ask_volumes" -> List(BigDecimal("9.12686646"), BigDecimal("0.05981955"), BigDecimal("1.00000000"))) ~
      ("ask_timestamps" -> List[BigDecimal]()) ~
      ("bid_prices" -> List(BigDecimal("462.49"), BigDecimal("462.48"), BigDecimal("462.47"))) ~
      ("bid_volumes" -> List(BigDecimal("0.03010000"), BigDecimal("4.03000000"), BigDecimal("16.49799877"))) ~
      ("bid_timestamps" -> List[BigDecimal]())
    withRunningKafka {
      Source.single((timeCollected, entity))
        .via(actor.orderbookFlow)
        .runWith(actor.selfSink)
      val msg = consumeFirstStringMessageFrom("bitstamp.polling.btcusd.orderbook")
      val result = parse(msg, true)
      assert(result == expected)
    }
  }
}
