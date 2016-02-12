package co.coinsmith.kafka.cryptocoin

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.math.BigDecimal.RoundingMode

import akka.actor.{Actor, Props}
import com.fasterxml.jackson.databind.ObjectMapper
import com.xeiam.xchange.Exchange
import com.xeiam.xchange.bitstamp.service.streaming.BitstampStreamingConfiguration
import com.xeiam.xchange.currency.CurrencyPair
import com.xeiam.xchange.dto.marketdata.{OrderBook, Trade}
import com.xeiam.xchange.dto.trade.LimitOrder
import com.xeiam.xchange.service.streaming.ExchangeEventType
import kafka.producer.KeyedMessage
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._


trait KafkaProducerMixin {
  val producer = KafkaCryptocoin.producer

  def send(topic: String, key: String, msg: String) {
    val data = new KeyedMessage[String, String](topic, key, msg)
    producer.send(data)
  }
}

class KafkaProducerActor extends KafkaProducerMixin with Actor {
  def receive = {
    case (topic: String, key: String, msg: String) =>
      send(topic, key, msg)
  }
}

class ExchangePollingActor(exchange: Exchange) extends Actor {
  val key = exchange.getExchangeSpecification.getExchangeName
  val marketDataService = exchange.getPollingMarketDataService
  val mapper = new ObjectMapper

  import context.dispatcher
  val tick = context.system.scheduler.schedule(0 seconds, 30 seconds, self, "tick")

  def receive = {
    case "tick" =>
      val ticker = marketDataService.getTicker(CurrencyPair.BTC_USD)
      val msg = mapper.writeValueAsString(ticker)
      context.actorOf(Props[KafkaProducerActor]) ! ("ticks", key, msg)
  }
}

class BitstampStreamingActor extends Actor {
  val exchange = ExchangeService.getExchange("bitstamp")
  val key = exchange.getExchangeSpecification.getExchangeName
  val streamConfig = new BitstampStreamingConfiguration
  val marketDataService = exchange.getStreamingExchangeService(streamConfig)
  marketDataService.connect

  def limitOrdersToLists(ob: List[LimitOrder]) = {
    ob.map { o => List(o.getLimitPrice, o.getTradableAmount)}
      .map { _ map { _.stripTrailingZeros } }
      .map { _ map { BigDecimal(_) } }
  }

  def orderBookToJson(ob: OrderBook, timeCollected: Long) = {
    val bids = limitOrdersToLists(ob.getBids.toList)
    val asks = limitOrdersToLists(ob.getAsks.toList)
    ("time_collected" -> timeCollected) ~
      ("ask_prices" -> asks.map { o => o(0) }) ~
      ("ask_volumes" -> asks.map { o => o(1) }) ~
      ("bid_prices" -> bids.map { o => o(0) }) ~
      ("bid_volumes" -> bids.map { o => o(1) })
  }

  override def preStart = getNextEvent

  def getNextEvent = {
    val event = marketDataService.getNextEvent
    val timeCollected = System.currentTimeMillis
    self ! (timeCollected, event.getEventType, event.getPayload)
  }

  def receive = {
    case (topic: String, key: String, json: JObject) =>
      val msg = compact(render(json))
      context.actorOf(Props[KafkaProducerActor]) ! (topic, key, msg)
      getNextEvent

    case (t: Long, ExchangeEventType.SUBSCRIBE_ORDERS, ob: OrderBook) =>
      val json = orderBookToJson(ob, t)
      self ! ("stream_orders", key, json)

    case (t: Long, ExchangeEventType.DEPTH, ob: OrderBook) =>
      val json = orderBookToJson(ob, t)
      self ! ("stream_depth", key, json)

    case (t: Long, ExchangeEventType.TRADE, trade: Trade) =>
      val price = BigDecimal(trade.getPrice.setScale(2, RoundingMode.HALF_DOWN)
        .bigDecimal.stripTrailingZeros)
      val volume = BigDecimal(trade.getTradableAmount.setScale(8, RoundingMode.HALF_DOWN)
        .bigDecimal.stripTrailingZeros)
      val json = ("time_collected" -> t) ~
        ("id" -> trade.getId) ~
        ("currencyPair" -> trade.getCurrencyPair.toString) ~
        ("price" -> price) ~ ("volume" -> volume)
      self ! ("stream_trades", key, json)
  }
}
