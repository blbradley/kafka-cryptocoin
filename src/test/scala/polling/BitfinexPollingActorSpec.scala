package polling

import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.testkit.TestActorRef
import akka.util.ByteString
import co.coinsmith.kafka.cryptocoin.polling.BitfinexPollingActor
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._


class BitfinexPollingActorSpec
  extends HTTPPollingActorSpec(ActorSystem("BitfinexPollingActorSpecSystem")) {
  implicit val materializer = ActorMaterializer()

  val actorRef = TestActorRef[BitfinexPollingActor]
  val actor = actorRef.underlyingActor
  actor.tick.cancel
  actor.orderbook.cancel

  "BitfinexPollingActor" should "process a ticker message" in {
    val timeCollected = Instant.ofEpochSecond(10L)
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

    val expected = ("mid" -> 464.845) ~
      ("bid" -> 464.8) ~
      ("ask" -> 464.89) ~
      ("last" -> 464.9) ~
      ("low" -> 452.96) ~
      ("high" -> 466.99) ~
      ("volume" -> 26373.95835286) ~
      ("timestamp" -> "2016-04-25T18:19:14.095383854Z") ~
      ("time_collected" -> timeCollected.toString)
    withRunningKafka {
      Source.single((timeCollected, entity))
        .via(actor.tickFlow)
        .runWith(actor.kafkaSink("ticks"))
      val msg = consumeFirstStringMessageFrom("ticks")
      val result = parse(msg, true)
      assert(result == expected)
    }
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

    val expected = ("time_collected" -> timeCollected.toString) ~
      ("ask_prices" -> List(464.12, 464.49, 464.63)) ~
      ("ask_volumes" -> List(1.457, 4.07481358, 4.07481358)) ~
      ("ask_timestamps" -> List(1461607308.0, 1461607942.0, 1461607944.0)) ~
      ("bid_prices" -> List(464.11, 463.87, 463.86)) ~
      ("bid_volumes" -> List(43.98077206, 21.3389, 12.5686)) ~
      ("bid_timestamps" -> List(1461607939.0, 1461607927.0, 1461607887.0))
    withRunningKafka {
      Source.single((timeCollected, entity))
        .via(actor.orderbookFlow)
        .runWith(actor.kafkaSink("orderbooks"))
      val msg = consumeFirstStringMessageFrom("orderbooks")
      val result = parse(msg, true)
      assert(result == expected)
    }
  }
}
