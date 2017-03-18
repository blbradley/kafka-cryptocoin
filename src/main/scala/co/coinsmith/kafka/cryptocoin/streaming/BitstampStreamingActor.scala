package co.coinsmith.kafka.cryptocoin.streaming

import java.time.Instant

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import co.coinsmith.kafka.cryptocoin.{Order, OrderBook, ProducerKey, Trade}
import co.coinsmith.kafka.cryptocoin.avro.InstantTypeMaps._
import co.coinsmith.kafka.cryptocoin.producer.Producer
import com.pusher.client.Pusher
import com.pusher.client.channel.ChannelEventListener
import com.pusher.client.connection.{ConnectionEventListener, ConnectionState, ConnectionStateChange}
import com.sksamuel.avro4s.RecordFormat
import com.typesafe.config.ConfigFactory
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._


case class BitstampStreamingTrade(id: Long, amount: Double, price: Double,
                                  tpe: Int, timestamp: String,
                                  buy_order_id: Long, sell_order_id: Long)
object BitstampStreamingTrade {
  implicit def toTrade(trade: BitstampStreamingTrade)(implicit timeCollected: Instant) =
    Trade(
      trade.price,
      trade.amount,
      Instant.ofEpochSecond(trade.timestamp.toLong),
      timeCollected,
      Some(trade.tpe match {
        case 0 => "bid"
        case 1 => "ask"
      }),
      Some(trade.id),
      Some(trade.buy_order_id),
      Some(trade.sell_order_id))
}

case class BitstampStreamingOrderBook(
  bids: List[List[String]],
  asks: List[List[String]],
  timestamp: Option[String]
)
object BitstampStreamingOrderBook {
  val toOrder = { o: List[String] => Order(o(0), o(1)) }

  implicit def toOrderBook(ob: BitstampStreamingOrderBook)(implicit timeCollected: Instant) =
    OrderBook(
      ob.bids map toOrder,
      ob.asks map toOrder,
      ob.timestamp map { _.toLong } map Instant.ofEpochSecond,
      Some(timeCollected)
    )
}

case class PusherEvent(channelName: String, eventName: String, timestamp: Instant, msg: String)
object PusherEvent {
  val format = RecordFormat[PusherEvent]
}

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
      receiver ! PusherEvent(channelName, eventName, timeCollected, data)
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
  implicit val formats = DefaultFormats

  def receive =  {
    case pe: PusherEvent => self forward (pe.channelName, pe.eventName, pe.timestamp, parse(pe.msg))
    case ("live_trades", "trade", t: Instant, json: JValue) =>
      implicit val timeCollected = t
      // some json processing required due to 'type' as a key name
      val trade = (json transformField{
        case ("type", v) => ("tpe", v)
      }).extract[BitstampStreamingTrade]
      sender ! ("trades", Trade.format.to(trade))
    case ("order_book", "data", t: Instant, json: JValue) =>
      implicit val timeCollected = t
      val ob = json.extract[BitstampStreamingOrderBook]
      sender ! ("orderbook", OrderBook.format.to(ob))
    case ("diff_order_book", "data", t: Instant, json: JValue) =>
      implicit val timeCollected = t
      val diff = json.extract[BitstampStreamingOrderBook]
      sender ! ("orderbook.updates", OrderBook.format.to(diff))
  }
}

class BitstampStreamingActor extends Actor with ActorLogging {
  val topicPrefix = "bitstamp.streaming.btcusd."

  val conf = ConfigFactory.load
  val preprocess = conf.getBoolean("kafka.cryptocoin.preprocess")

  val pusher = context.actorOf(Props[BitstampPusherActor])
  val protocol = context.actorOf(Props[BitstampPusherProtocol])

  override def preStart = {
    pusher ! self
    pusher ! Connect
  }

  def receive = {
    case (topic: String, value: Object) =>
      Producer.send(topicPrefix + topic, value)
    case pe : PusherEvent =>
      val key = ProducerKey("bitstamp", Producer.uuid)
      Producer.send("streaming.websocket.raw", ProducerKey.format.to(key), PusherEvent.format.to(pe))

      if (preprocess) {
        protocol ! pe
      }
  }
}
