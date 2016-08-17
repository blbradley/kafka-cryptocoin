package co.coinsmith.kafka.cryptocoin.streaming

import java.net.URI
import java.time.Instant

import akka.actor.{Actor, ActorLogging, Props}
import co.coinsmith.kafka.cryptocoin.{Order, OrderBook, Tick}
import co.coinsmith.kafka.cryptocoin.producer.ProducerBehavior
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._


class BitfinexWebsocketProtocol extends Actor with ActorLogging {
  implicit val formats = DefaultFormats

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

  def isEvent(event: JValue): Boolean = event.findField {
    case ("event", eventName: JString) => true
    case _ => false
  } match {
    case None => false
    case _ => true
  }

  def toTick(arr: List[Double]) =
    Tick(
      arr(6), arr(0), arr(2),
      Some(arr(8)), Some(arr(9)), None,
      Some(arr(7)), None,
      Some(arr(1)), Some(arr(3)),
      Some(arr(4)), Some(arr(5))
    )

  def toOrder(order: List[Double]) = Order(order(1), order(2), Some(order(0).toLong))

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
      getChannelName(channelId) match {
        case "book" =>
          val orders = JArray(data).extract[List[List[Double]]] map toOrder
          val ob = OrderBook(
            orders filter { _.volume > 0 },
            orders filter { _.volume < 0 }
          )
          sender ! ("orderbook.snapshots", OrderBook.format.to(ob))
        case "trades" =>
          val json = data map { t =>
            val seqOrId = t(0) match {
              case seq: JString => ("seq" -> seq)
              case id: JInt => ("id" -> id)
              case _ => throw new Exception(s"Trade snapshot processing error for $t")
            }
            seqOrId ~ ("timestamp" -> t(1)) ~ ("price" -> t(2)) ~ ("volume" -> t(3))
          }
          sender ! ("trades.snapshots", JArray(json))
        case _ => throw new Exception("Snapshot does not have nested array structure.")
      }
    case (t: Instant, JArray(JInt(channelId) :: JString(updateType) :: xs)) =>
      sender ! (topic(channelId, updateType), JArray(xs))
    case (t: Instant, JArray(JInt(channelId) :: xs)) =>
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
