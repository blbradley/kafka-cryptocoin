package co.coinsmith.kafka.cryptocoin.streaming

import java.time.Instant

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import co.coinsmith.kafka.cryptocoin.producer.ProducerBehavior
import com.pusher.client.Pusher
import com.pusher.client.channel.ChannelEventListener
import com.pusher.client.connection.{ConnectionEventListener, ConnectionState, ConnectionStateChange}
import org.json4s.JsonAST._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._


class BitstampPusherActor extends Actor with ActorLogging {
  var receiver: ActorRef = _

  val pusher = new Pusher("de504dc5763aeef9ff52")
  val connectionEventListener = new ConnectionEventListener {
    override def onError(s: String, s1: String, e: Exception) = {
      log.error(e, "There was a problem connecting!")
    }

    override def onConnectionStateChange(change: ConnectionStateChange) = {
      log.info("State changed from " + change.getPreviousState + " to " + change.getCurrentState)
      change.getCurrentState match {
        case ConnectionState.DISCONNECTED => self ! Connect
        case _ =>
      }
    }
  }
  val channelEventListener = new ChannelEventListener {
    override def onSubscriptionSucceeded(channelName: String) = {
      log.info("Subscribed to channel {}", channelName)
    }

    override def onEvent(channelName: String, eventName: String, data: String) = {
      val timeCollected = Instant.now
      log.debug("Received event {} on channel {}: {}", channelName, eventName, data)
      receiver ! (channelName, eventName, timeCollected, parse(data))
    }
  }

  pusher.subscribe("live_trades", channelEventListener, "trade")
  pusher.subscribe("order_book", channelEventListener, "data")
  pusher.subscribe("diff_order_book", channelEventListener, "data")

  def connect: Unit = {
    pusher.connect(connectionEventListener, ConnectionState.ALL)
  }

  def receive = {
    case actor: ActorRef => receiver = actor
    case Connect => connect
  }
}

class BitstampPusherProtocol extends Actor {
  def mergeInstant(key: String, t: Instant, json: JValue) = {
    render(key -> t.toString) merge json
  }

  def receive =  {
    case ("live_trades", "trade", t: Instant, json: JValue) =>
      val trade = json transformField {
        case ("timestamp", JString(t)) => ("timestamp", Instant.ofEpochSecond(t.toLong).toString)
        case ("amount", v) => ("volume", v)
      }
      sender ! ("trades", mergeInstant("time_collected", t, trade))
    case ("order_book", "data", t: Instant, json: JValue) =>
      val ob = json transform {
        case JString(v) => JDecimal(BigDecimal(v))
      }
      sender ! ("orderbook", mergeInstant("time_collected", t, ob))
    case ("diff_order_book", "data", t: Instant, json: JValue) =>
      val diff = json transformField {
        case ("timestamp", JString(t)) => ("timestamp", t.toLong)
      } transform {
        case JInt(t) => JString(Instant.ofEpochSecond(t.toLong).toString)
        case JString(v) => JDecimal(BigDecimal(v))
      }
      sender ! ("orderbook.updates", mergeInstant("time_collected", t, diff))
  }
}

class BitstampStreamingActor extends Actor with ActorLogging with ProducerBehavior {
  val topicPrefix = "bitstamp.streaming.btcusd."

  val pusher = context.actorOf(Props[BitstampPusherActor])
  val protocol = context.actorOf(Props[BitstampPusherProtocol])

  override def preStart = {
    pusher ! self
    pusher ! Connect
  }

  def receive = producerBehavior orElse {
    case (channelName: String, eventName: String, t: Instant, json: JValue) =>
      protocol ! (channelName, eventName, t, json)
  }
}
