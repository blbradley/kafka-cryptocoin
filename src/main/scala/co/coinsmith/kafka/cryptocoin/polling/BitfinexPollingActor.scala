package co.coinsmith.kafka.cryptocoin.polling

import java.time.Instant

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ResponseEntity
import akka.stream.scaladsl.Flow
import co.coinsmith.kafka.cryptocoin.{Order, Utils}
import org.json4s.JsonAST._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._


class BitfinexPollingActor extends HTTPPollingActor {
  val topicPrefix = "bitfinex.polling.btcusd."
  val pool = Http(context.system).cachedHostConnectionPoolHttps[String]("api.bitfinex.com")

  val tickFlow = Flow[(Instant, ResponseEntity)].map { case (t, entity) =>
    parse(responseEntityToString(entity)) transform {
      case JString(v) => JDecimal(BigDecimal(v))
    } transformField {
      case JField("timestamp", JDecimal(v)) =>
        val parts = v.toString.split('.').map { _.toLong }
        val timestamp = Instant.ofEpochSecond(parts(0), parts(1))
        JField("timestamp", JString(timestamp.toString))
      case JField("last_price", v) => JField("last", v)
    } merge render("time_collected" -> t.toString)
  }

  val orderbookFlow = Flow[(Instant, ResponseEntity)].map { case (t, entity) =>
    val json = parse(responseEntityToString(entity)) transform {
      case JString(v) => JDecimal(BigDecimal(v))
    } transformField {
      case JField("amount", v) => JField("volume", v)
    }
    val asks = (json \ "asks").extract[List[Order]]
    val bids = (json \ "bids").extract[List[Order]]
    Utils.orderBookToJson(None, t, asks, bids)
  }

  def receive = {
    case "tick" =>
      request("/v1/pubticker/btcusd")
        .via(tickFlow)
        .runWith(kafkaSink(topicPrefix + "ticks"))
    case "orderbook" =>
      request("/v1/book/btcusd")
        .via(orderbookFlow)
        .runWith(kafkaSink(topicPrefix + "orderbook"))
  }
}
