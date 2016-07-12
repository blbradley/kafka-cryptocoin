package co.coinsmith.kafka.cryptocoin.polling

import java.time.Instant

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ResponseEntity
import akka.stream.scaladsl.Flow
import org.json4s.JsonAST._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._


class OKCoinPollingActor extends HTTPPollingActor {
  val topicPrefix = "okcoin.polling.btcusd."
  val pool = Http(context.system).cachedHostConnectionPoolHttps[String]("www.okcoin.cn")

  val tickFlow = Flow[(Instant, ResponseEntity)].map { t =>
    val json = parse(responseEntityToString(t._2))
    val ticker = json \ "ticker" transformField {
      case JField(key, JString(v)) => JField(key, JDecimal(BigDecimal(v)))
    } transformField {
      case JField("buy", v) => JField("bid", v)
      case JField("sell", v) => JField("ask", v)
      case JField("vol", v) => JField("volume", v)
    }
    val timestamp = Instant.ofEpochSecond((json \ "date").extract[String].toLong)
    val times = ("time_collected" -> t._1.toString) ~ ("timestamp" -> timestamp.toString)
    times merge ticker
  }

  val orderbookFlow = Flow[(Instant, ResponseEntity)].map { t =>
    val json = parse(responseEntityToString(t._2))
    render("time_collected" -> t._1.toString) merge json
  }

  def receive = {
    case "tick" =>
      request("/api/v1/ticker.do?symbol=btc_cny")
        .via(tickFlow)
        .runWith(kafkaSink(topicPrefix + "ticks"))
    case "orderbook" =>
      request("/api/v1/depth.do??symbol=btc_cny")
        .via(orderbookFlow)
        .runWith(kafkaSink(topicPrefix + "orderbook"))
  }
}
