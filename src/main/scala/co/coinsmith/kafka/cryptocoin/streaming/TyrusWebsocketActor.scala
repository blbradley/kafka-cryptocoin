package co.coinsmith.kafka.cryptocoin.streaming

import java.net.URI
import java.time.Instant
import javax.websocket.MessageHandler.Whole
import javax.websocket._

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import org.glassfish.tyrus.client.ClientManager
import org.json4s.jackson.JsonMethods._


class TyrusWebsocketActor(uri: URI) extends Actor with ActorLogging {
  var remote: RemoteEndpoint.Async = _
  var receiver : ActorRef = _

  val cec = ClientEndpointConfig.Builder.create().build
  val client = ClientManager.createClient
  val endpoint = new Endpoint {
    override def onOpen(session: Session, config: EndpointConfig) = {
      try {
        session.addMessageHandler(new Whole[String] {
          override def onMessage(message: String) {
            val timeCollected = Instant.now
            log.debug("Received message {} at time {}", message, timeCollected.toString)
            receiver ! (timeCollected, parse(message))
          }
        })

        remote = session.getAsyncRemote
      } catch {
        case ex: Exception => throw ex
      }
    }

    override def onClose(session: Session, closeReason: CloseReason) = {
      log.info("Websocket for {} disconnected with reason: {}", receiver.path.name, closeReason)
    }

    override def onError(session: Session, thr: Throwable) = {
      log.error(thr, "Websocket error for {}", receiver.path.name)
      throw thr
    }
  }

  def connect = {
    client.connectToServer(endpoint, cec, uri)
    log.info("Websocket for {} connected.", receiver.path.name)
  }

  def receive = {
    case actor: ActorRef => receiver = actor
    case Connect => connect
    case msg: String => remote.sendText(msg)
  }
}

object TyrusWebsocketActor {
  def props(uri: URI) = Props(new TyrusWebsocketActor(uri))
}
