package co.coinsmith.kafka.cryptocoin

import scala.concurrent.Future
import scala.math.BigDecimal.RoundingMode
import scala.util.{Failure, Success}

import akka.actor.{Props, Actor}
import com.xeiam.xchange.Exchange
import com.xeiam.xchange.dto.marketdata.{Trade, OrderBook, Ticker}
import com.xeiam.xchange.service.streaming.{ExchangeStreamingConfiguration, ExchangeEventType}
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL.WithBigDecimal._


class StreamingActor extends Actor {
  def receive = {
    case e: Exchange =>
      ExchangeService.getStreamingConfig(e) match {
        case Some(sc) =>
          val name = ExchangeService.getNameFromExchange(e).toLowerCase
          context.actorOf(Props(classOf[ExchangeStreamingActor], e, sc), name)
        case None =>
      }
  }
}

class ExchangeStreamingActor(exchange: Exchange, streamConfig: ExchangeStreamingConfiguration)
  extends Actor {
  val key = exchange.getExchangeSpecification.getExchangeName
  val marketDataService = exchange.getStreamingExchangeService(streamConfig)

  import context.dispatcher
  override def preStart = {
    Future {
      marketDataService.connect
    }.onComplete {
      case _ => getNextEvent
    }
  }

  def getNextEvent = Future {
    marketDataService.getNextEvent
  }.onComplete {
    case Success(e) =>
      val timeCollected = System.currentTimeMillis
      self ! (timeCollected, e.getEventType, e.getPayload)
    case Failure(ex) => throw ex
  }

  def receive = {
    case (_, ExchangeEventType.DISCONNECT, _) =>
      throw new Exception("Received WebSocket disconnect event")

    case (topic: String, key: String, json: JObject) =>
      val msg = compact(render(json))
      context.actorOf(Props[KafkaProducerActor]) ! (topic, key, msg)
      getNextEvent

    case (t: Long, ExchangeEventType.TICKER, tick: Ticker) =>
      val json = Utils.tickerToJson(tick, t)
      self ! ("stream_ticks", key, json)

    case (t: Long, ExchangeEventType.SUBSCRIBE_ORDERS, ob: OrderBook) =>
      val json = Utils.orderBookToJson(ob, t)
      self ! ("stream_orders", key, json)

    case (t: Long, ExchangeEventType.DEPTH, ob: OrderBook) =>
      val json = Utils.orderBookToJson(ob, t)
      self ! ("stream_depth", key, json)

    case (t: Long, ExchangeEventType.TRADE, trade: Trade) =>
      val timestamp = Option(trade.getTimestamp).map { _.getTime }
      val orderType = Option(trade.getType).map { _.toString.toLowerCase }
      val price = BigDecimal(trade.getPrice.setScale(2, RoundingMode.HALF_DOWN)
        .bigDecimal.stripTrailingZeros)
      val volume = BigDecimal(trade.getTradableAmount.setScale(8, RoundingMode.HALF_DOWN)
        .bigDecimal.stripTrailingZeros)
      val json = ("timestamp" -> timestamp) ~
        ("time_collected" -> t) ~
        ("id" -> trade.getId) ~
        ("type" -> orderType) ~
        ("currencyPair" -> trade.getCurrencyPair.toString) ~
        ("price" -> price) ~ ("volume" -> volume)
      self ! ("stream_trades", key, json)
  }
}
