package co.coinsmith.kafka.cryptocoin.streaming

import java.time.Instant

import akka.persistence.{PersistentActor, Recovery}

case class SaveWebsocketMessage(timestamp: Instant, exchange: String, msg: String)

case class WebsocketMessage(timestamp: Instant, exchange: String, msg: String)

class PersistentStreamingActor extends PersistentActor {
  override def persistenceId: String = "streaming"
  override def recovery = Recovery.none

  override def receiveRecover: Receive = {
    case _ =>
  }

  override def receiveCommand: Receive = {
    case SaveWebsocketMessage(timestamp, exchange, msg) =>
      persist(WebsocketMessage(timestamp, exchange, msg)) { received =>
        context.system.eventStream.publish(received)
      }
  }
}
