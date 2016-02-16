package co.coinsmith.kafka.cryptocoin

import scala.collection.JavaConversions._
import java.util.ServiceLoader

import com.xeiam.xchange.service.streaming.ExchangeStreamingConfiguration
import com.xeiam.xchange.{Exchange, ExchangeFactory}


object ExchangeService {
  private val exchangeLoader = ServiceLoader.load(classOf[Exchange])
  private val names = exchangeLoader.iterator.toSeq.map(_.getClass.getName)
  private val exchanges = names.map(ExchangeFactory.INSTANCE.createExchange)

  private def getNameFromExchange(exchange: Exchange) = {
    exchange.getClass.getSimpleName.dropRight(8)
  }

  def getExchanges() = {
    exchanges
  }

  private val streamingConfigLoader = ServiceLoader.load(classOf[ExchangeStreamingConfiguration])
  private val streamingConfigs = exchanges.map { getNameFromExchange(_) }
    .map { n => (n, None) }.toMap ++
    streamingConfigLoader.iterator.toSeq
      .map { c => (c.getClass.getSimpleName, Some(c)) }
      .map {
        case (name, c) if name contains "Exchange" => (name.dropRight(30), c)
        case (name, c) => (name.dropRight(22), c)
      }.toMap

  def getStreamingConfig(exchange: Exchange) = {
    streamingConfigs(getNameFromExchange(exchange))
  }
}
