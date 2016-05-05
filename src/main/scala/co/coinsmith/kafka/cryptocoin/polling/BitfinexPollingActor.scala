package co.coinsmith.kafka.cryptocoin.polling

import scala.concurrent.Future
import java.time.Instant

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpEntity, HttpRequest, ResponseEntity}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.ByteString
import co.coinsmith.kafka.cryptocoin.{KafkaProducer, Order, Utils}
import org.json4s.JsonAST._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._


class BitfinexPollingActor extends HTTPPollingActor {
  val key = "BitFinex"
  implicit val materializer = ActorMaterializer()
  import context.dispatcher
  val pool = Http(context.system).cachedHostConnectionPoolHttps[String]("api.bitfinex.com")

  val chunkedFlow = Flow[(Instant, ResponseEntity)].map {
    case (t, HttpEntity.Chunked(_, chunks)) =>
      chunks.runFold(ByteString()) { (s, chunk) => s ++ chunk.data }
        .map { s => (t, s.utf8String) }
    case (_, e) =>
      throw new Exception(s"Chunked response failed with entity ${e}")
  }

  def request(uri: String) =
    Source.single(HttpRequest(uri = uri) -> "")
      .via(pool)
      .via(responseFlow)
      .via(chunkedFlow)

  def topicSink(topic: String) = Sink.foreach[Future[(Instant, String)]] { f =>
    f.onSuccess { case (t, s) => self ! (t, topic, s) }
  }

  def receive = {
    case "tick" => request("/v1/pubticker/btcusd").runWith(topicSink("ticks"))
    case "orderbook" => request("/v1/book/btcusd").runWith(topicSink("orderbooks"))
    case (t: Instant, "ticks", data: String) =>
      val json = parse(data) transform {
        case JString(v) => JDecimal(BigDecimal(v))
      } transformField {
        case JField("timestamp", JDecimal(v)) =>
          val parts = v.toString.split('.').map { _.toLong }
          val timestamp = Instant.ofEpochSecond(parts(0), parts(1))
          JField("timestamp", JString(timestamp.toString))
        case JField("last_price", v) => JField("last", v)
      } merge render("time_collected" -> t.toString)
      self ! ("ticks", json)
    case (t: Instant, "orderbooks", data: String) =>
      val json = parse(data) transform {
        case JString(v) => JDecimal(BigDecimal(v))
      } transformField {
        case JField("amount", v) => JField("volume", v)
      }
      val asks = (json \ "asks").extract[List[Order]]
      val bids = (json \ "bids").extract[List[Order]]
      val orderbook = Utils.orderBookToJson(None, t, asks, bids)
      self ! ("orderbooks", orderbook)
    case (topic: String, json: JValue) =>
      val msg = compact(render(json))
      KafkaProducer.send(topic, key, msg)
  }
}
