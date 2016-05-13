package co.coinsmith.kafka.cryptocoin

import akka.actor.{Actor, Props}
import co.coinsmith.kafka.cryptocoin.streaming.{BitstampStreamingActor, OKCoinStreamingActor}


class StreamingActor extends Actor {
  val bitstamp = context.actorOf(Props[BitstampStreamingActor], "bitstamp")
  val okcoin = context.actorOf(Props[OKCoinStreamingActor], "okcoin")

  bitstamp ! "connect"

  def receive = {
    case _ =>
  }
}
