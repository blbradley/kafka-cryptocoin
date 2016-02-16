package co.coinsmith.kafka.cryptocoin

import scala.collection.JavaConversions._
import scala.concurrent.duration._

import akka.actor.{Actor, Props}
import com.fasterxml.jackson.databind.ObjectMapper
import com.xeiam.xchange.Exchange
import org.json4s.jackson.JsonMethods._

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
      val ticker = marketDataService.getTicker(currencyPair)
      val timeCollected = System.currentTimeMillis
      val json = Utils.tickerToJson(ticker, timeCollected)
      val msg = compact(render(json))
      context.actorOf(Props[KafkaProducerActor]) ! ("ticks", key, msg)

    case "orderbook" =>
      val ob = marketDataService.getOrderBook(currencyPair)
      val timeCollected = System.currentTimeMillis
      val json = Utils.orderBookToJson(ob, timeCollected)
      val msg = compact(render(json))
      context.actorOf(Props[KafkaProducerActor]) ! ("orderbooks", key, msg)
  }
}
