package co.coinsmith.kafka.cryptocoin

import akka.actor.{Actor, Props}
import co.coinsmith.kafka.cryptocoin.streaming.OKCoinStreamingActor


class StreamingActor extends Actor {
  val okcoin = context.actorOf(Props[OKCoinStreamingActor], "okcoin")
  def receive = {
    case _ =>
  }
}
