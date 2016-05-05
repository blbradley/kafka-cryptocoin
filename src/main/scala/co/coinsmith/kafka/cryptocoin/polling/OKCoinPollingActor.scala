package co.coinsmith.kafka.cryptocoin.polling

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import java.time.Instant

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, ResponseEntity}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.ByteString
import co.coinsmith.kafka.cryptocoin.KafkaProducer
import org.json4s.JsonAST._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._


class OKCoinPollingActor extends HTTPPollingActor {
  val key = "OKCoin"
  val pool = Http(context.system).cachedHostConnectionPoolHttps[String]("www.okcoin.cn")

  val tickFlow = Flow[(Instant, ResponseEntity)].map { t =>
    val data = concatByteStringSource(t._2.dataBytes)
    val json = parse(data.utf8String)
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
    val data = concatByteStringSource(t._2.dataBytes)
    val json = parse(data.utf8String)
    render("time_collected" -> t._1.toString) merge json
  }

  def concatByteStringSource(dataBytes: Source[ByteString, Any]): ByteString = {
    Await.result(dataBytes.runReduce(_ ++ _), Duration.Inf)
  }

  def kafkaSink(topic: String) = Sink.foreach[JValue] {
    case json => KafkaProducer.send(topic, key, compact(render(json)))
  }

  def receive = {
    case "tick" =>
      Source.single(HttpRequest(uri = "/api/v1/ticker.do?symbol=btc_cny") -> "")
        .via(pool)
        .via(responseFlow)
        .via(tickFlow)
        .runWith(kafkaSink("ticks"))
    case "orderbook" =>
      Source.single(HttpRequest(uri = "/api/v1/depth.do??symbol=btc_cny") -> "")
        .via(pool)
        .via(responseFlow)
        .via(orderbookFlow)
        .runWith(kafkaSink("orderbooks"))
  }
}
