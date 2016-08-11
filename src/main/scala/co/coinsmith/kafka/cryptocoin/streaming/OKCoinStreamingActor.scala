package co.coinsmith.kafka.cryptocoin.streaming

import java.net.URI
import java.time._

import akka.actor.{Actor, ActorLogging, Props}
import co.coinsmith.kafka.cryptocoin.{Order, OrderBook, Tick}
import co.coinsmith.kafka.cryptocoin.producer.ProducerBehavior
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._


case class Data(timeCollected: Instant, channel: String, data: JValue)

case class OKCoinStreamingTick(buy: String, high: String, last: String, low: String, sell: String, timestamp: String, vol: String)
object OKCoinStreamingTick {
  implicit def toTick(tick: OKCoinStreamingTick) =
    Tick(
      tick.last.toDouble,
      tick.buy.toDouble,
      tick.sell.toDouble,
      Instant.ofEpochMilli(tick.timestamp.toLong),
      Some(tick.high.toDouble),
      Some(tick.low.toDouble),
      None,
      Some(tick.vol.replace(",", "").toDouble)
    )
}

case class OKCoinStreamingOrderBook(bids: List[List[Double]], asks: List[List[Double]], timestamp: String)
object OKCoinStreamingOrderBook {
  val toOrder = { o: List[Double] => Order(o(0), o(1)) }

  implicit def toOrderBook(ob: OKCoinStreamingOrderBook) =
    OrderBook(ob.bids map toOrder, ob.asks map toOrder, Some(Instant.ofEpochMilli(ob.timestamp.toLong)))
}

class OKCoinWebsocketProtocol extends Actor with ActorLogging {
  implicit val formats = DefaultFormats

  def mergeInstant(key: String, t: Instant, json: JValue) = {
    render(key -> t.toString) merge json
  }

  def receive = {
    case (t: Instant, events: JArray) =>
      // OKCoin websocket responses are an array of multiple events
      events match {
        case JArray(arr) => arr.foreach { event => self forward (t, event) }
        case _ => new Exception("Message did not contain array.")
      }
    case (t: Instant, JObject(JField("channel", JString(channel)) ::
                     JField("success", JString(success)) :: Nil)) =>
      log.info("Added channel {} at time {}.", channel, t)

    case (t: Instant, JObject(JField("channel", JString(channel)) ::
                   JField("success", JString(success)) ::
                   JField("error_code", JInt(errorCode)) :: Nil)) =>
      log.error("Adding channel {} failed at time {}. Error code {}.", channel, t, errorCode)

    case (t: Instant, JObject(JField("channel", JString(channel)) :: JField("data", data) :: Nil)) =>
      self forward Data(t, channel, data)

    case Data(t, "ok_sub_spotcny_btc_ticker", data) =>
      val tick = data.extract[OKCoinStreamingTick]
      sender ! ("ticks", Tick.format.to(tick))

    case Data(t, "ok_sub_spotcny_btc_depth_60", data) =>
      val ob = data.extract[OKCoinStreamingOrderBook]
      sender ! ("orderbook", OrderBook.format.to(ob))

    case Data(t, "ok_sub_spotcny_btc_trades", data: JArray) =>
      val json = data.transform {
        case JArray(JString(id) :: JString(p) :: JString(v) :: JString(time) :: JString(kind) :: Nil) =>
          val zone = ZoneId.of("Asia/Shanghai")
          val collectedZoned = ZonedDateTime.ofInstant(t, ZoneOffset.UTC)
            .withZoneSameInstant(zone)
          var tradeZoned = LocalTime.parse(time).atDate(collectedZoned.toLocalDate).atZone(zone)
          if ((tradeZoned compareTo collectedZoned) > 0) {
            // correct date if time collected happens right after midnight
            tradeZoned = tradeZoned minusDays 1
          }
          val timestamp = tradeZoned.withZoneSameInstant(ZoneOffset.UTC)
          ("timestamp" -> timestamp.toString) ~
            ("time_collected" -> t.toString) ~
            ("id" -> id.toLong) ~
            ("price" -> BigDecimal(p)) ~
            ("volume" -> BigDecimal(v)) ~
            ("type" -> kind)
      }
      sender ! ("trades", json)
  }
}

class OKCoinStreamingActor extends Actor with ActorLogging with ProducerBehavior {
  val topicPrefix = "okcoin.streaming.btcusd."
  val uri = new URI("wss://real.okcoin.cn:10440/websocket/okcoinapi")

  val websocket = context.actorOf(WebsocketActor.props(uri))
  val protocol = context.actorOf(Props[OKCoinWebsocketProtocol])

  val channels = List(
    ("event" -> "addChannel") ~ ("channel" -> "ok_sub_spotcny_btc_ticker"),
    ("event" -> "addChannel") ~ ("channel" -> "ok_sub_spotcny_btc_depth_60"),
    ("event" -> "addChannel") ~ ("channel" -> "ok_sub_spotcny_btc_trades")
  )
  val initMessage = compact(render(channels))

  override def preStart = {
    websocket ! self
    websocket ! Connect
    websocket ! initMessage
    log.debug("Sent initialization message: {}", initMessage)
  }

  def receive = producerBehavior orElse {
    case (t: Instant, json: JValue) => protocol ! (t, json)
  }
}
