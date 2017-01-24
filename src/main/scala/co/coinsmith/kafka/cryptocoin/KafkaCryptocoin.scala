package co.coinsmith.kafka.cryptocoin

import akka.actor.{ActorSystem, Props}


object KafkaCryptocoin {
  implicit val system = ActorSystem("KafkaCryptocoinSystem")
  def main(args: Array[String]) {
    val streamingActor = system.actorOf(Props[StreamingActor], "streaming")
    val pollingActor = system.actorOf(Props[PollingActor], "polling")
  }
}
