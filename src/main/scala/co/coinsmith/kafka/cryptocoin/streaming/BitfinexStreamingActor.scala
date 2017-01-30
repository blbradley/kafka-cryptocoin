package co.coinsmith.kafka.cryptocoin.streaming

import java.net.URI
import java.time.Instant

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{Actor, ActorLogging, AllForOneStrategy, Props}
import akka.http.scaladsl.model.ws.TextMessage
import co.coinsmith.kafka.cryptocoin.{Order, OrderBook, Tick, Trade}
import co.coinsmith.kafka.cryptocoin.producer.ProducerBehavior
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._


class BitfinexWebsocketProtocol extends Actor with ActorLogging {
  implicit val formats = DefaultFormats

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

  def receive = {
    case (t, JObject(JField("event", JString("subscribed")) ::
                     JField("channel", JString(channelName)) ::
                     JField("chanId", JInt(channelId)) ::
                     xs)) =>
      log.info("Received subscription event response for channel {} with ID {}", channelName, channelId)
      subscribed += (channelId -> channelName)
    case (t, event: JValue) if isEvent(event) =>
      log.info("Received event message: {}", compact(render(event)))
    case (t: Instant, JArray(JInt(channelId) :: JString("hb") :: Nil)) =>
      log.debug("Received heartbeat message for channel ID {}", channelId)
    case (t: Instant, JArray(JInt(channelId) :: JArray(data) :: Nil)) =>
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
    case (t: Instant, JArray(JInt(channelId) :: JString(updateType) :: xs)) =>
      implicit val timeCollected = t
      val topic = updateType match {
        case "tu" => "trades"
        case "te" => "trades.executions"
      }
      val trade = toTrade(JArray(xs))
      sender ! (topic, Trade.format.to(trade))
    case (t: Instant, JArray(JInt(channelId) :: xs)) =>
      implicit val timeCollected = t
      getChannelName(channelId) match {
        case "book" =>
          val order = toOrder(JArray(xs).extract[List[Double]])
          sender ! ("orderbook", Order.format.to(order))
        case "ticker" =>
          val tick = toTick(JArray(xs).extract[List[Double]])
          sender ! ("ticker", Tick.format.to(tick))
      }
    case m => throw new Exception(s"Unhandled message: $m")
  }
}

class BitfinexStreamingActor extends Actor with ActorLogging with ProducerBehavior {
  implicit val actorSystem = context.system

  val topicPrefix = "bitfinex.streaming.btcusd."
  val uri = new URI("wss://api2.bitfinex.com:3000/ws")

  override val supervisorStrategy = AllForOneStrategy() {
    case _: Exception => Escalate
    case t => super.supervisorStrategy.decider.applyOrElse(t, (_: Any) => Escalate)
  }

  val channels = List(
    ("event" -> "subscribe") ~ ("channel" -> "book") ~ ("pair" -> "BTCUSD")
      ~ ("prec" -> "R0") ~ ("len" -> "100"),
    ("event" -> "subscribe") ~ ("channel" -> "trades") ~ ("pair" -> "BTCUSD"),
    ("event" -> "subscribe") ~ ("channel" -> "ticker") ~ ("pair" -> "BTCUSD")
  )
  val messages = channels.map(j => compact(render(j))).map(s => TextMessage(s))

  val websocket = new AkkaWebsocket(uri, messages, self)
  val protocol = context.actorOf(Props[BitfinexWebsocketProtocol])

  def receive = producerBehavior orElse {
    case (t: Instant, msg: String) => protocol ! (t, parse(msg))
  }
}
