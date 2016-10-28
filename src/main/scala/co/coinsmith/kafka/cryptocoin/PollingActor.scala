package co.coinsmith.kafka.cryptocoin

import akka.actor.{Actor, ActorLogging, Props}
import com.typesafe.config.ConfigFactory
import polling.{BitfinexPollingActor, BitstampPollingActor, OKCoinPollingActor}


class PollingActor extends Actor with ActorLogging {
  val conf = ConfigFactory.load
  val configuredExchanges = conf.getStringList("kafka.cryptocoin.exchanges")

  val supportedExchanges = Map(
    "bitfinex" -> Props[BitfinexPollingActor],
    "bitstamp" -> Props[BitstampPollingActor],
    "okcoin" -> Props[OKCoinPollingActor]
  )

  val exchangeActors = supportedExchanges filterKeys { name =>
    configuredExchanges contains name
  } map { case (name, props) =>
    name -> context.actorOf(props, name)
  }

  if (exchangeActors.isEmpty) {
    log.warning("Polling has no active exchanges.")
  }

  exchangeActors foreach { _._2 ! "start" }

  def receive = {
    case _ =>
  }
}
