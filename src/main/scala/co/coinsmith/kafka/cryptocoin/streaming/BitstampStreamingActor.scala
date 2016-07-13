package co.coinsmith.kafka.cryptocoin.streaming

import java.time.Instant

import akka.actor.{Actor, ActorLogging}
import co.coinsmith.kafka.cryptocoin.KafkaProducer
import co.coinsmith.kafka.cryptocoin.producer.ProducerBehavior
import com.pusher.client.Pusher
import com.pusher.client.channel.ChannelEventListener
import com.pusher.client.connection.{ConnectionEventListener, ConnectionState, ConnectionStateChange}
import org.json4s.JsonAST._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._


class BitstampStreamingActor extends Actor with ActorLogging with ProducerBehavior {
  val topicPrefix = "bitstamp.streaming.btcusd."

  val pusher = new Pusher("de504dc5763aeef9ff52")
  val connectionEventListener = new ConnectionEventListener {
    override def onError(s: String, s1: String, e: Exception) = {
      log.error(e, "There was a problem connecting!")
    }

    override def onConnectionStateChange(change: ConnectionStateChange) = {
      log.info("State changed from " + change.getPreviousState + " to " + change.getCurrentState)
      change.getCurrentState match {
        case ConnectionState.DISCONNECTED => self ! "connect"
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
      self ! (channelName, eventName, timeCollected, parse(data))
    }
  }

  pusher.subscribe("live_trades", channelEventListener, "trade")
  pusher.subscribe("order_book", channelEventListener, "data")
  pusher.subscribe("diff_order_book", channelEventListener, "data")

  def connect: Unit = {
    pusher.connect(connectionEventListener, ConnectionState.ALL)
  }

  def mergeInstant(key: String, t: Instant, json: JValue) = {
    render(key -> t.toString) merge json
  }

  def receive = producerBehavior orElse {
    case Connect => connect
    case ("live_trades", "trade", t: Instant, json: JValue) =>
      val trade = json transformField {
        case ("timestamp", JString(t)) => ("timestamp", Instant.ofEpochSecond(t.toLong).toString)
        case ("amount", v) => ("volume", v)
      }
      self ! ("trades", mergeInstant("time_collected", t, trade))
    case ("order_book", "data", t: Instant, json: JValue) =>
      val ob = json transform {
        case JString(v) => JDecimal(BigDecimal(v))
      }
      self ! ("orderbook", mergeInstant("time_collected", t, ob))
    case ("diff_order_book", "data", t: Instant, json: JValue) =>
      val diff = json transformField {
        case ("timestamp", JString(t)) => ("timestamp", t.toLong)
      } transform {
        case JInt(t) => JString(Instant.ofEpochSecond(t.toLong).toString)
        case JString(v) => JDecimal(BigDecimal(v))
      }
      self ! ("orderbook.updates", mergeInstant("time_collected", t, diff))
  }
}
