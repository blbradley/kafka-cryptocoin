package co.coinsmith.kafka.cryptocoin.streaming

import scala.math.BigInt
import java.net.URI
import java.time.Instant

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{Actor, ActorLogging, AllForOneStrategy, Props}
import akka.http.scaladsl.model.ws.TextMessage
import co.coinsmith.kafka.cryptocoin._
import co.coinsmith.kafka.cryptocoin.producer.Producer
import com.typesafe.config.ConfigFactory
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._


case class Unsubscribed(channel: String)

class BitfinexWebsocketProtocol extends Actor with ActorLogging {
  implicit val formats = DefaultFormats

  val conf = ConfigFactory.load
  val preprocess = conf.getBoolean("kafka.cryptocoin.preprocess")

  var subscribed = Map.empty[BigInt, String]
  def getChannelName(channelId: BigInt) = subscribed(channelId)

  def isEvent(event: JValue): Boolean = event.findField {
    case ("event", eventName: JString) => true
    case _ => false
  } match {
    case None => false
    case _ => true
  }

  def toTick(arr: List[Double])(implicit timeCollected: Instant) =
    Tick(
      arr(6), arr(0), arr(2), timeCollected,
      Some(arr(8)), Some(arr(9)), None,
      Some(arr(7)), None,
      Some(arr(1)), Some(arr(3)),
      Some(arr(4)), Some(arr(5))
    )

  def toOrder(order: List[Double])(implicit timeCollected: Instant) =
    Order(order(1), order(2), Some(order(0).toLong), Some(timeCollected))

  def toDouble(v: JValue) = v match {
    case JInt(i) => i.toDouble
    case JDouble(d) => d
    case _ => throw new Exception(s"Could not convert to double: $v")
  }

  // price and volume can be JInt or JDouble
  def toTrade(trade: JValue)(implicit timeCollected: Instant) = trade match {
    case JArray(JString(seq) :: JInt(id) :: JInt(timestamp) :: price :: volume :: Nil) =>
      Trade(toDouble(price), toDouble(volume), Instant.ofEpochSecond(timestamp.toLong), timeCollected, tid = Some(id.toLong), seq = Some(seq))
    case JArray(JString(seq) :: JInt(timestamp) :: price :: volume :: Nil) =>
      Trade(toDouble(price), toDouble(volume), Instant.ofEpochSecond(timestamp.toLong), timeCollected, seq = Some(seq))
    case JArray(JInt(id) :: JInt(timestamp) :: price :: volume :: Nil) =>
      Trade(toDouble(price), toDouble(volume), Instant.ofEpochSecond(timestamp.toLong), timeCollected,  tid = Some(id.toLong))
    case _ => throw new Exception(s"Trade snapshot processing error for $trade")
  }

  def handleInfoEvent(code: Int, msg: String) = code match {
    case 20060 => log.warning(msg)
    case 20061 =>
      log.warning(msg)
      for (id <- subscribed.keys) {
        log.warning("Sending unsubscribe request for channel {}", getChannelName(id),id)
        sender ! ("event" -> "unsubscribe") ~ ("chanId" -> id)
      }
  }

  def handleErrorEvent(code: Int, msg: String) = code match {
    case 10400 => log.error("During unsubscription: {}", msg)
  }

  def receive = {
    case (t, JObject(JField("event", JString("subscribed")) ::
                     JField("channel", JString(channelName)) ::
                     JField("chanId", JInt(channelId)) ::
                     xs)) =>
      log.info("Received subscription event response for channel {} with ID {}", channelName, channelId)
      subscribed += (channelId -> channelName)
    case (t, JObject(JField("event", JString("info")) ::
                     JField("code", JInt(code)) ::
                     JField("msg", JString(msg)) :: Nil)) => handleInfoEvent(code.toInt, msg)
    case (t, JObject(JField("event", JString("unsubscribed")) ::
                     JField("status", JString("OK")) ::
                     JField("chanId", JInt(channelId)) :: Nil)) =>
      val channelName = getChannelName(channelId)
      log.warning("Unsubscribe response for channel {}: OK", getChannelName(channelId))
      sender ! Unsubscribed(channelName)
      subscribed = subscribed - channelId
    case (t, JObject(JField("event", JString("error")) ::
                     JField("code", JInt(code)) ::
                     JField("msg", JString(msg)) :: Nil)) => handleErrorEvent(code.toInt, msg)
    case (t, event: JValue) if isEvent(event) =>
      log.info("Received event message: {}", compact(render(event)))
    case (t: Instant, JArray(JInt(channelId) :: JString("hb") :: Nil)) =>
      log.debug("Received heartbeat message for channel ID {}", channelId)
    case (t: Instant, JArray(JInt(channelId) :: JArray(data) :: Nil)) if preprocess =>
      implicit val timeCollected = t
      getChannelName(channelId) match {
        case "book" =>
          val orders = JArray(data).extract[List[List[Double]]] map toOrder
          val ob = OrderBook(
            orders filter { _.volume > 0 },
            orders filter { _.volume < 0 },
            timeCollected = Some(t)
          )
          sender ! ("orderbook.snapshots", OrderBook.format.to(ob))
        case "trades" => data map toTrade foreach { trade =>
          sender ! ("trades.snapshots", Trade.format.to(trade))
        }
      }
    case (t: Instant, JArray(JInt(channelId) :: JString(updateType) :: xs)) if preprocess =>
      implicit val timeCollected = t
      val topic = updateType match {
        case "tu" => "trades"
        case "te" => "trades.executions"
      }
      val trade = toTrade(JArray(xs))
      sender ! (topic, Trade.format.to(trade))
    case (t: Instant, JArray(JInt(channelId) :: xs)) if preprocess =>
      implicit val timeCollected = t
      getChannelName(channelId) match {
        case "book" =>
          val order = toOrder(JArray(xs).extract[List[Double]])
          sender ! ("orderbook", Order.format.to(order))
        case "ticker" =>
          val tick = toTick(JArray(xs).extract[List[Double]])
          sender ! ("ticker", Tick.format.to(tick))
      }
  }
}

class BitfinexStreamingActor extends Actor with ActorLogging {
  implicit val actorSystem = context.system

  val topicPrefix = "bitfinex.streaming.btcusd."
  val uri = new URI("wss://api2.bitfinex.com:3000/ws")

  override val supervisorStrategy = AllForOneStrategy() {
    case _: Exception => Escalate
    case t => super.supervisorStrategy.decider.applyOrElse(t, (_: Any) => Escalate)
  }

  val channelSubscriptionMessages = Map(
    "book" -> ("event" -> "subscribe") ~ ("channel" -> "book") ~ ("pair" -> "BTCUSD")
      ~ ("prec" -> "R0") ~ ("len" -> "100"),
    "trades" -> ("event" -> "subscribe") ~ ("channel" -> "trades") ~ ("pair" -> "BTCUSD"),
    "ticker" -> ("event" -> "subscribe") ~ ("channel" -> "ticker") ~ ("pair" -> "BTCUSD")
  ).mapValues(j => compact(render(j))).mapValues(s => TextMessage(s))

  val websocket = new AkkaWebsocket(uri, channelSubscriptionMessages.values.toList, self)
  val protocol = context.actorOf(Props[BitfinexWebsocketProtocol])
  websocket.connect

  def receive = {
    case json: JValue =>
      websocket.send(TextMessage(compact(render(json))))
    case e: Unsubscribed =>
      websocket.send(channelSubscriptionMessages(e.channel))
    case (topic: String, value: Object) =>
      Producer.send(topicPrefix + topic, value)
    case (t: Instant, msg: String) =>
      val exchange = "bitfinex"
      val event = ExchangeEvent(t, exchange, msg)
      Producer.send("streaming.websocket.raw", exchange, ExchangeEvent.format.to(event))

      protocol ! (t, parse(msg))
  }
}
