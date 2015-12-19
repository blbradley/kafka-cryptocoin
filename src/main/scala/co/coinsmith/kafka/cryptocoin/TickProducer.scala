package co.coinsmith.kafka.cryptocoin

import com.fasterxml.jackson.databind.ObjectMapper
import com.xeiam.xchange.Exchange
import com.xeiam.xchange.currency.CurrencyPair
import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage


class TickProducer(exchange: Exchange,
                   producer: Producer[String, String]) extends Runnable {
  val marketDataService = exchange.getPollingMarketDataService
  val mapper = new ObjectMapper
  val topic = "ticks"
  val name = exchange.getExchangeSpecification.getExchangeName

  def run() {
    val ticker = marketDataService.getTicker(CurrencyPair.BTC_USD)
    val msg = mapper.writeValueAsString(ticker)
    val data = new KeyedMessage[String, String](topic, name, msg)
    producer.send(data)
  }
}
