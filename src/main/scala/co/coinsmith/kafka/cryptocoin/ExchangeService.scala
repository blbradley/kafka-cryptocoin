package co.coinsmith.kafka.cryptocoin

import scala.collection.JavaConversions._
import java.util.ServiceLoader

import com.xeiam.xchange.{Exchange, ExchangeFactory}


object ExchangeService {
  private val loader = ServiceLoader.load(classOf[Exchange])
  private val names = loader.iterator.toSeq.map(_.getClass.getName)
  private val exchanges = names.map(ExchangeFactory.INSTANCE.createExchange)

  def getExchanges() = {
    exchanges
  }

  def getExchange(name: String) = {
    exchanges.filter(_.getExchangeSpecification.getExchangeName.toLowerCase == name).head
  }
}
