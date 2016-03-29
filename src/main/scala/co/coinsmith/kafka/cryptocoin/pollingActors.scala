package co.coinsmith.kafka.cryptocoin

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import java.time.Instant

import akka.Done
import akka.actor.{Actor, ActorLogging, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.ByteString
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._


class PollingActor extends Actor {
  val bitstamp = context.actorOf(Props[BitstampPollingActor])
  val bitfinex = context.actorOf(Props[BitfinexPollingActor])
  def receive = {
    case _ =>
  }
}

abstract class HTTPPollingActor extends Actor with ActorLogging {
  import context.dispatcher
  val tick = context.system.scheduler.schedule(0 seconds, 30 seconds, self, "tick")
  val orderbook = context.system.scheduler.schedule(0 seconds, 30 seconds, self, "orderbook")

  val responseFlow = Flow[(Try[HttpResponse], String)].map {
    case (Success(HttpResponse(statusCode, _, entity, _)), _) =>
      val timeCollected = Instant.now
      log.debug("Request returned status code {} with entity {}",  statusCode, entity)
      (timeCollected, entity)
    case (Failure(response), _) =>
      throw new Exception(s"Request failed with response ${response}")
  }
}

class BitfinexPollingActor extends HTTPPollingActor{
  implicit val formats = DefaultFormats
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
      context.actorOf(Props[KafkaProducerActor]) ! (topic, key, msg)
  }
}

class BitstampPollingActor extends HTTPPollingActor {
  implicit val formats = DefaultFormats
  val key = "bitstamp"
  implicit val materializer = ActorMaterializer()
  import context.dispatcher
  val pool = Http(context.system).cachedHostConnectionPoolHttps[String]("www.bitstamp.net")

  def sink: Sink[(Try[HttpResponse], String), Future[Done]] = {
    Sink.foreach {
      case (Success(HttpResponse(statusCode, _, entity, _)), _) =>
        val timeCollected = Instant.now
        log.debug("Request returned status code {} with entity {}",  statusCode, entity)
        self ! (timeCollected, entity)
      case (Failure(response), _) =>
        log.debug("Request failed with response {}", response)
    }
  }

  def receive = {
    case "tick" =>
      Source.single(HttpRequest(uri = "/api/ticker/") -> "")
        .via(pool)
        .runWith(sink)
    case "orderbook" =>
      Source.single(HttpRequest(uri = "/api/order_book/") -> "")
        .via(pool)
        .runWith(sink)
    case (t: Instant, HttpEntity.Strict(_, data)) =>
      val json = parse(data.utf8String).transformField {
        case JField("timestamp", JString(t)) => JField("timestamp", Instant.ofEpochSecond(t.toLong).toString)
        case JField(key, JString(value)) => JField(key, JDecimal(BigDecimal(value)))
      } merge render("time_collected" -> t.toString)
      self ! ("ticks", json)

    case (t: Instant, HttpEntity.Default(_, _, data)) =>
      data.runWith(Sink.fold[ByteString, ByteString](ByteString())(_ ++ _))
        .onSuccess {
          case s =>
            val json = parse(s.utf8String).transformField {
              case JField("timestamp", JString(t)) => JField("timestamp", Instant.ofEpochSecond(t.toLong).toString)
            }
            val timestamp = (json \ "timestamp").extract[String]
            val asks = (json \ "asks").extract[List[List[String]]]
              .map { o => new Order(BigDecimal(o(0)), BigDecimal(o(1))) }
            val bids = (json \ "bids").extract[List[List[String]]]
              .map { o => new Order(BigDecimal(o(0)), BigDecimal(o(1))) }
            val orderbook = Utils.orderBookToJson(Some(timestamp), t, asks, bids)
            self ! ("orderbooks", orderbook)
        }
    case (topic: String, json: JValue) =>
      val msg = compact(render(json))
      context.actorOf(Props[KafkaProducerActor]) ! (topic, key, msg)
  }
}
