package co.coinsmith.kafka.cryptocoin

import akka.actor.{ActorSystem, Props}


object KafkaCryptocoin {
  val system = ActorSystem("PollingSystem")
  def main(args: Array[String]) {
    val streamingActor = system.actorOf(Props[StreamingActor], "streaming")
    val pollingActor = system.actorOf(Props[PollingActor], "polling")
  }
}
