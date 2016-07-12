package co.coinsmith.kafka.cryptocoin.streaming

import java.net.URI
import java.time.Instant
import javax.websocket.MessageHandler.Whole
import javax.websocket._

import akka.actor.{Actor, ActorLogging}
import org.glassfish.tyrus.client.ClientManager
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._


abstract class ExchangeStreamingActor extends Actor with ActorLogging {
  val topicPrefix: String
  val uri : URI

  val cec = ClientEndpointConfig.Builder.create().build
  val client = ClientManager.createClient
  val endpoint = new Endpoint {
    override def onOpen(session: Session, config: EndpointConfig) = {
      try {
        // setup message handler and subscribe to channels
        session.addMessageHandler(new Whole[String] {
          override def onMessage(message: String) {
            val timeCollected = Instant.now
            log.debug("Received message {} at time {}", message, timeCollected.toString)
            self ! (timeCollected, parse(message))
          }
        })
        subscribe(session)
      } catch {
        case ex: Exception => throw ex
      }
    }
  }

  def subscribe(session: Session)

  def connect = {
    client.connectToServer(endpoint, cec, uri)
    log.info("Websocket connected.")
  }

  def mergeInstant(key: String, t: Instant, json: JValue) = {
    render(key -> t.toString) merge json
  }
}
