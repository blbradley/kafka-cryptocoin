package co.coinsmith.kafka.cryptocoin

import scala.collection.JavaConversions._
import scala.math.BigDecimal.RoundingMode

import com.fasterxml.jackson.databind.ObjectMapper
import com.xeiam.xchange.Exchange
import com.xeiam.xchange.bitstamp.service.streaming.BitstampStreamingConfiguration
import com.xeiam.xchange.currency.CurrencyPair
import com.xeiam.xchange.dto.marketdata.{OrderBook, Trade}
import com.xeiam.xchange.dto.trade.LimitOrder
import com.xeiam.xchange.service.streaming.ExchangeEventType
import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._


abstract class ProducerMixin(producer: Producer[String, String]) extends Runnable {
  val key: String

  def send(topic: String, msg: String) {
    val data = new KeyedMessage[String, String](topic, key, msg)
    producer.send(data)
  }
}

class TickProducer(exchange: Exchange,
                   producer: Producer[String, String])
  extends ProducerMixin(producer) {
  val key = exchange.getExchangeSpecification.getExchangeName
  val marketDataService = exchange.getPollingMarketDataService
  val topic = "ticks"
  val mapper = new ObjectMapper

  def run() {
    val ticker = marketDataService.getTicker(CurrencyPair.BTC_USD)
    val msg = mapper.writeValueAsString(ticker)
    send(topic, msg)
  }
}

class BitstampStreamingProducer(producer: Producer[String, String])
  extends ProducerMixin(producer) {
  val exchange = ExchangeService.getExchange("bitstamp")
  val key = exchange.getExchangeSpecification.getExchangeName
  val streamConfig = new BitstampStreamingConfiguration
  val marketDataService = exchange.getStreamingExchangeService(streamConfig)
  marketDataService.connect
  val mapper = new ObjectMapper

  def limitOrdersToLists(ob: List[LimitOrder]) = {
    ob.map { o => List(o.getLimitPrice, o.getTradableAmount)}
      .map { _ map { _.stripTrailingZeros } }
      .map { _ map { BigDecimal(_) } }
  }

  def run() {
    while (true) {
      val event = marketDataService.getNextEvent
      val topic = event.getEventType match {
        case ExchangeEventType.SUBSCRIBE_ORDERS => "stream_orders"
        case ExchangeEventType.TRADE => "stream_trades"
        case ExchangeEventType.DEPTH => "stream_depth"
        case _ => null
      }
      val msg = event.getPayload match {
        case ob: OrderBook =>
          val bids = limitOrdersToLists(ob.getBids.toList)
          val asks = limitOrdersToLists(ob.getAsks.toList)
          val json = ("ask_prices" -> asks.map { o => o(0) }) ~
            ("ask_volumes" -> asks.map { o => o(1) }) ~
            ("bid_prices" -> bids.map { o => o(0) }) ~
            ("bid_volumes" -> bids.map { o => o(1) })
          compact(render(json))
        case trade: Trade =>
          val price = trade.getPrice.setScale(2, RoundingMode.HALF_DOWN)
            .bigDecimal.stripTrailingZeros
          val volume = trade.getTradableAmount.setScale(8, RoundingMode.HALF_DOWN)
            .bigDecimal.stripTrailingZeros
          val fixed = Trade.Builder.from(trade).price(price).tradableAmount(volume).build
          mapper.writeValueAsString(fixed)
      }
      send(topic, msg)
    }
  }
}
