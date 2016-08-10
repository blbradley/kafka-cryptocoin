package co.coinsmith.kafka.cryptocoin

import akka.actor.{Actor, Props}
import polling.{BitfinexPollingActor, BitstampPollingActor, OKCoinPollingActor}


class PollingActor extends Actor {
  val bitstamp = context.actorOf(Props[BitstampPollingActor], "bitstamp")
  val bitfinex = context.actorOf(Props[BitfinexPollingActor], "bitfinex")
  val okcoin = context.actorOf(Props[OKCoinPollingActor], "okcoin")

//  bitfinex ! "start"
//  bitstamp ! "start"
//  okcoin ! "start"

  def receive = {
    case _ =>
  }
}
