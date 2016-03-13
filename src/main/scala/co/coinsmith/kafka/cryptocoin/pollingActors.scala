package co.coinsmith.kafka.cryptocoin

import scala.collection.JavaConversions._
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
import com.fasterxml.jackson.databind.ObjectMapper
import com.xeiam.xchange.Exchange
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._


class PollingActor extends Actor {
  val bitstamp = context.actorOf(Props[BitstampPollingActor])
  def receive = {
    case e: Exchange =>
      val name = ExchangeService.getNameFromExchange(e).toLowerCase
      if (name != "bitstamp") {
        context.actorOf(Props(classOf[ExchangePollingActor], e), name)
      }
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
        case JField("timestamp", JString(t)) => JField("timestamp", JLong(t.toLong * 1000))
        case JField(key, JString(value)) => JField(key, JDecimal(BigDecimal(value)))
      } merge render("time_collected" -> t.toEpochMilli)
      self ! ("ticks", json)

    case (t: Instant, HttpEntity.Default(_, _, data)) =>
      data.runWith(Sink.fold[ByteString, ByteString](ByteString())(_ ++ _))
        .onSuccess {
          case s =>
            val json = parse(s.utf8String).transformField {
              case JField("timestamp", JString(t)) => JField("timestamp", JLong(t.toLong * 1000))
            } transform {
              case JString(v) => JDecimal(BigDecimal(v))
            }
            val timestamp = Some((json \ "timestamp").extract[Long])
            val asks = (json \ "asks").extract[List[List[BigDecimal]]]
            val bids = (json \ "bids").extract[List[List[BigDecimal]]]
            val orderbook = Utils.orderBookToJson(timestamp, t.toEpochMilli, asks, bids)
            self ! ("orderbooks", orderbook)
        }
    case (topic: String, json: JValue) =>
      val msg = compact(render(json))
      context.actorOf(Props[KafkaProducerActor]) ! (topic, key, msg)
  }
}

class ExchangePollingActor(exchange: Exchange) extends Actor {
  val key = exchange.getExchangeSpecification.getExchangeName
  val currencyPair = exchange.getMetaData.getMarketMetaDataMap
    .map { case (pair, _) => pair }
    .filter { p => p.baseSymbol == "BTC"}
    .head
  val marketDataService = exchange.getPollingMarketDataService
  val mapper = new ObjectMapper

  import context.dispatcher
  val tick = context.system.scheduler.schedule(0 seconds, 30 seconds, self, "tick")
  val orderbook = context.system.scheduler.schedule(0 seconds, 30 seconds, self, "orderbook")

  def receive = {
    case "tick" =>
      import context.dispatcher
      Future { marketDataService.getTicker(currencyPair) }
        .onComplete {
          case Success(ticker) =>
            val timeCollected = System.currentTimeMillis
            val json = Utils.tickerToJson(ticker, timeCollected)
            val msg = compact(render(json))
            context.actorOf(Props[KafkaProducerActor]) ! ("ticks", key, msg)
          case Failure(ex) => throw ex
        }

    case "orderbook" =>
      Future { marketDataService.getOrderBook(currencyPair) }
        .onComplete {
          case Success(ob) =>
            val timeCollected = System.currentTimeMillis
            val json = Utils.orderBookXChangeToJson(ob, timeCollected)
            val msg = compact(render(json))
            context.actorOf(Props[KafkaProducerActor]) ! ("orderbooks", key, msg)
          case Failure(ex) => throw ex
        }
  }
}
