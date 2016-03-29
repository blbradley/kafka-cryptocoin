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
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._


class PollingActor extends Actor {
  val bitstamp = context.actorOf(Props[BitstampPollingActor])
  def receive = {
    case _ =>
  }
}

class BitstampPollingActor extends Actor with ActorLogging {
  implicit val formats = DefaultFormats
  val key = "bitstamp"
  implicit val materializer = ActorMaterializer()
  val pool = Http(context.system).cachedHostConnectionPoolHttps[String]("www.bitstamp.net")

  import context.dispatcher
  val tick = context.system.scheduler.schedule(0 seconds, 30 seconds, self, "tick")
  val orderbook = context.system.scheduler.schedule(0 seconds, 30 seconds, self, "orderbook")

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
