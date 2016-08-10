package co.coinsmith.kafka.cryptocoin

import akka.actor.{Actor, Props}
import co.coinsmith.kafka.cryptocoin.streaming.{BitfinexStreamingActor, BitstampStreamingActor, Connect, OKCoinStreamingActor}


class StreamingActor extends Actor {
  val bitfinex = context.actorOf(Props[BitfinexStreamingActor], "bitfinex")
  val bitstamp = context.actorOf(Props[BitstampStreamingActor], "bitstamp")
  val okcoin = context.actorOf(Props[OKCoinStreamingActor], "okcoin")

//  bitfinex ! Connect
//  bitstamp ! Connect
//  okcoin ! Connect

  def receive = {
    case _ =>
  }
}
