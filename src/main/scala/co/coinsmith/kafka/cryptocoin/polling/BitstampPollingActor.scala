package co.coinsmith.kafka.cryptocoin.polling

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import java.time.Instant

import akka.Done
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpEntity, HttpRequest, HttpResponse}
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import co.coinsmith.kafka.cryptocoin.producer.ProducerBehavior
import co.coinsmith.kafka.cryptocoin.{KafkaProducer, Order, Utils}
import org.json4s.JsonAST._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._


class BitstampPollingActor extends HTTPPollingActor with ProducerBehavior {
  val topicPrefix = "bitstamp.polling.btcusd."
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

  def receive = producerBehavior orElse {
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
        case JField("timestamp", JString(t)) => JField("timestamp", JString(Instant.ofEpochSecond(t.toLong).toString))
        case JField(key, JString(value)) => JField(key, JDecimal(BigDecimal(value)))
      } merge render("time_collected" -> t.toString)
      self ! ("ticks", json)

    case (t: Instant, HttpEntity.Default(_, _, data)) =>
      data.runWith(Sink.fold[ByteString, ByteString](ByteString())(_ ++ _))
        .onSuccess {
          case s =>
            val json = parse(s.utf8String).transformField {
              case JField("timestamp", JString(t)) => JField("timestamp", JString(Instant.ofEpochSecond(t.toLong).toString))
            }
            val timestamp = (json \ "timestamp").extract[String]
            val asks = (json \ "asks").extract[List[List[String]]]
              .map { o => new Order(BigDecimal(o(0)), BigDecimal(o(1))) }
            val bids = (json \ "bids").extract[List[List[String]]]
              .map { o => new Order(BigDecimal(o(0)), BigDecimal(o(1))) }
            val orderbook = Utils.orderBookToJson(Some(timestamp), t, asks, bids)
            self ! ("orderbook", orderbook)
        }
  }
}
