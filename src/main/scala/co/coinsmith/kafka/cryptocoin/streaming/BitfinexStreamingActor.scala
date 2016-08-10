package co.coinsmith.kafka.cryptocoin.streaming

import java.net.URI
import java.time.Instant

import akka.actor.{Actor, ActorLogging, Props}
import co.coinsmith.kafka.cryptocoin.producer.ProducerBehavior
import org.json4s.JsonAST._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._


class BitfinexWebsocketProtocol extends Actor with ActorLogging {
  var subscribed = Map.empty[BigInt, String]
  def getChannelName(channelId: BigInt) = subscribed(channelId)
  def topic(channelId: BigInt, updateType: String): String = {
    (getChannelName(channelId), updateType) match {
      case ("book", "snapshot")   => "orderbook.snapshots"
      case ("book", "update")     => "orderbook"
      case ("trades", "snapshot") => "trades.snapshots"
      case ("trades", "tu")       => "trades"
      case ("trades", "te")       => "trades.executions"
      case ("ticker", "ticker")   => "ticker"
    }
  }

  def processOrder(order: JValue) = {
    ("id" -> order(0)) ~
      ("price" -> order(1)) ~
      ("volume" -> order(2))
  }

  def isEvent(event: JValue): Boolean = event.findField {
    case ("event", eventName: JString) => true
    case _ => false
  } match {
    case None => false
    case _ => true
  }

  def receive = {
    case (t, JObject(JField("event", JString("subscribed")) ::
                     JField("channel", JString(channelName)) ::
                     JField("chanId", JInt(channelId)) ::
                     JField("pair", JString("BTCUSD")) ::
                     xs)) =>
      log.info("Received subscription event response for channel {} with ID {}", channelName, channelId)
      subscribed += (channelId -> channelName)
    case (t, event: JValue) if isEvent(event) =>
      log.info("Received event message: {}", compact(render(event)))
    case (t: Instant, JArray(JInt(channelId) :: JString("hb") :: Nil)) =>
      log.debug("Received heartbeat message for channel ID {}", channelId)
    case (t: Instant, JArray(JInt(channelId) :: JArray(data) :: Nil)) =>
      val json = getChannelName(channelId) match {
        case "book" => data map processOrder
        case "trades" => data map { t =>
          val seqOrId = t(0) match {
            case seq: JString => ("seq" -> seq)
            case id: JInt => ("id" -> id)
            case _ => throw new Exception(s"Trade snapshot processing error for $t")
          }
          seqOrId ~ ("timestamp" -> t(1)) ~ ("price" -> t(2)) ~ ("volume" -> t(3))
        }
        case _ => throw new Exception("Snapshot does not have nested array structure.")
      }
      sender ! (topic(channelId, "snapshot"), JArray(json))
    case (t: Instant, JArray(JInt(channelId) :: JString(updateType) :: xs)) =>
      sender ! (topic(channelId, updateType), JArray(xs))
    case (t: Instant, JArray(JInt(channelId) :: xs)) =>
      val updateType = xs.length match {
        case 3 => "update"
        case 10 => "ticker"
      }
      sender ! (topic(channelId, updateType), JArray(xs))
    case m => throw new Exception(s"Unhandled message: $m")
  }
}

class BitfinexStreamingActor extends Actor with ActorLogging with ProducerBehavior {
  val topicPrefix = "bitfinex.streaming.btcusd."
  val uri = new URI("wss://api2.bitfinex.com:3000/ws")

  val websocket = context.actorOf(WebsocketActor.props(uri))
  val protocol = context.actorOf(Props[BitfinexWebsocketProtocol])

  val channels = List(
    ("event" -> "subscribe") ~ ("channel" -> "book") ~ ("pair" -> "BTCUSD")
      ~ ("prec" -> "R0") ~ ("len" -> "100"),
    ("event" -> "subscribe") ~ ("channel" -> "trades") ~ ("pair" -> "BTCUSD"),
    ("event" -> "subscribe") ~ ("channel" -> "ticker") ~ ("pair" -> "BTCUSD")
  )

  override def preStart = {
    websocket ! self
    websocket ! Connect
    for (channel <- channels) {
      val msg = compact(render(channel))
      websocket ! msg
      log.debug("Sent subscription message: {}", msg)
    }
  }

  def receive = producerBehavior orElse {
    case (t: Instant, json: JValue) => protocol ! (t, json)
  }
}
