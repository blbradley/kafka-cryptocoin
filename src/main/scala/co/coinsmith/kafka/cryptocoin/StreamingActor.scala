package co.coinsmith.kafka.cryptocoin

import akka.actor.{Actor, ActorLogging, Props}
import co.coinsmith.kafka.cryptocoin.streaming.{BitfinexStreamingActor, BitstampStreamingActor, OKCoinStreamingActor}
import com.typesafe.config.ConfigFactory


class StreamingActor extends Actor with ActorLogging {
  val conf = ConfigFactory.load
  val configuredExchanges = conf.getStringList("kafka.cryptocoin.exchanges")

  val supportedExchanges = Map(
    "bitfinex" -> Props[BitfinexStreamingActor],
    "bitstamp" -> Props[BitstampStreamingActor],
    "okcoin" -> Props[OKCoinStreamingActor]
  )

  val exchangeActors = supportedExchanges filterKeys { name =>
    configuredExchanges contains name
  } map { case (name, props) =>
    name -> context.actorOf(props, name)
  }

  if (exchangeActors.isEmpty) {
    log.warning("Streaming has no active exchanges.")
  }

  def receive = {
    case _ =>
  }
}
