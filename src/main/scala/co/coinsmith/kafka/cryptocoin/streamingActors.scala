package co.coinsmith.kafka.cryptocoin

import scala.math.BigDecimal.RoundingMode

import akka.actor.{Props, Actor}
import com.xeiam.xchange.Exchange
import com.xeiam.xchange.dto.marketdata.{Trade, OrderBook, Ticker}
import com.xeiam.xchange.service.streaming.ExchangeEventType
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL.WithBigDecimal._


class StreamingActor extends Actor {
  def receive = {
    case e: Exchange =>
      val name = ExchangeService.getNameFromExchange(e).toLowerCase
      context.actorOf(Props(classOf[ExchangeStreamingActor], e), name)
  }
}

class ExchangeStreamingActor(exchange: Exchange) extends Actor {
  val key = exchange.getExchangeSpecification.getExchangeName
  val streamConfig = ExchangeService.getStreamingConfig(exchange)
  val marketDataService = streamConfig match {
    case Some(c) => Some(exchange.getStreamingExchangeService(c))
    case None => None
  }

  override def preStart = {
    marketDataService match {
      case Some(s) =>
        s.connect
        getNextEvent
      case None =>
    }
  }

  def getNextEvent = {
    val event = marketDataService.get.getNextEvent
    val timeCollected = System.currentTimeMillis
    self ! (timeCollected, event.getEventType, event.getPayload)
  }

  def receive = {
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
