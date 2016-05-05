package co.coinsmith.kafka.cryptocoin

import akka.actor.{Actor, Props}
import polling.{BitfinexPollingActor, BitstampPollingActor}


class PollingActor extends Actor {
  val bitstamp = context.actorOf(Props[BitstampPollingActor])
  val bitfinex = context.actorOf(Props[BitfinexPollingActor])
  def receive = {
    case _ =>
  }
}
