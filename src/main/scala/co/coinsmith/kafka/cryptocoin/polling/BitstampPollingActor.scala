package co.coinsmith.kafka.cryptocoin.polling

import java.time.Instant

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ResponseEntity
import akka.stream.scaladsl.Flow
import co.coinsmith.kafka.cryptocoin.producer.ProducerBehavior
import co.coinsmith.kafka.cryptocoin.{Order, Tick, Utils}
import org.json4s.JsonAST._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._

case class BitstampPollingTick(
  high: String,
  last: String,
  timestamp: String,
  bid: String,
  vwap: String,
  volume: String,
  low: String,
  ask: String,
  open: Double
)
object BitstampPollingTick {
  implicit def toTick(tick: BitstampPollingTick) =
    Tick(BigDecimal(tick.last), BigDecimal(tick.bid), BigDecimal(tick.ask), BigDecimal(tick.high), BigDecimal(tick.low), BigDecimal(tick.vwap), BigDecimal(tick.volume), BigDecimal(tick.open), Instant.ofEpochSecond(tick.timestamp.toLong).toString)
}

class BitstampPollingActor extends HTTPPollingActor with ProducerBehavior {
  val topicPrefix = "bitstamp.polling.btcusd."
  val pool = Http(context.system).cachedHostConnectionPoolHttps[String]("www.bitstamp.net")

  val tickFlow = Flow[(Instant, ResponseEntity)].map { case (t, entity) =>
//    val json = parse(responseEntityToString(entity)).transformField {
//      case JField("timestamp", JString(t)) => JField("timestamp", JString(Instant.ofEpochSecond(t.toLong).toString))
//      case JField(key, JString(value)) => JField(key, JDecimal(BigDecimal(value)))
//    } merge render("time_collected" -> t.toString)

    val msg = parse(responseEntityToString(entity))
    val tick = msg.extract[BitstampPollingTick]

    ("ticks", Tick.format.to(tick))
  }

  val orderbookFlow = Flow[(Instant, ResponseEntity)].map { case (t, entity) =>
    val json = parse(responseEntityToString(entity)).transformField {
      case JField("timestamp", JString(t)) => JField("timestamp", JString(Instant.ofEpochSecond(t.toLong).toString))
    }
    val timestamp = (json \ "timestamp").extract[String]
    val asks = (json \ "asks").extract[List[List[String]]]
      .map { o => new Order(BigDecimal(o(0)), BigDecimal(o(1))) }
    val bids = (json \ "bids").extract[List[List[String]]]
      .map { o => new Order(BigDecimal(o(0)), BigDecimal(o(1))) }
    val orderbook = Utils.orderBookToJson(Some(timestamp), t, asks, bids)
    ("orderbook", orderbook)
  }

  def receive = periodicBehavior orElse producerBehavior orElse {
    case "tick" =>
      request("/api/ticker/")
        .via(tickFlow)
        .runWith(selfSink)
    case "orderbook" =>
      request("/api/order_book/")
        .via(orderbookFlow)
        .runWith(selfSink)
  }
}
