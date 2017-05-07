package co.coinsmith.kafka.cryptocoin.streaming

import scala.concurrent.duration._
import java.net.URI
import java.time._

import akka.actor.{Actor, ActorLogging, Props}
import akka.http.scaladsl.model.ws.TextMessage
import co.coinsmith.kafka.cryptocoin.producer.Producer
import co.coinsmith.kafka.cryptocoin._
import com.typesafe.config.ConfigFactory
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._


case class Data(timeCollected: Instant, channel: String, data: JValue)

case class OKCoinStreamingTick(buy: String, high: String, last: String, low: String,
                               sell: String, timestamp: String, vol: String)
object OKCoinStreamingTick {
  implicit def toTick(tick: OKCoinStreamingTick)(implicit timeCollected: Instant)  =
    Tick(
      tick.last.toDouble, tick.buy.toDouble, tick.sell.toDouble, timeCollected,
      Some(tick.high.toDouble), Some(tick.low.toDouble),
      volume = Some(tick.vol.replace(",", "").toDouble),
      timestamp = Some(Instant.ofEpochMilli(tick.timestamp.toLong))
    )
}

case class OKCoinStreamingOrderBook(bids: List[List[Double]], asks: List[List[Double]], timestamp: String)
object OKCoinStreamingOrderBook {
  val toOrder = { o: List[Double] => Order(o(0), o(1)) }

  implicit def toOrderBook(ob: OKCoinStreamingOrderBook)(implicit timeCollected: Instant)  =
    OrderBook(
      ob.bids map toOrder, ob.asks map toOrder,
      Some(Instant.ofEpochMilli(ob.timestamp.toLong)), Some(timeCollected)
    )
}

class OKCoinWebsocketProtocol extends Actor with ActorLogging {
  implicit val formats = DefaultFormats

  val conf = ConfigFactory.load
  val preprocess = conf.getBoolean("kafka.cryptocoin.preprocess")

  def adjustTimestamp(timeCollected: Instant, time: String) = {
    val zone = ZoneId.of("Asia/Shanghai")
    val collectedZoned = ZonedDateTime.ofInstant(timeCollected, ZoneOffset.UTC)
      .withZoneSameInstant(zone)
    var tradeZoned = LocalTime.parse(time).atDate(collectedZoned.toLocalDate).atZone(zone)
    if ((tradeZoned compareTo collectedZoned) > 0) {
      // correct date if time collected happens right after midnight
      tradeZoned = tradeZoned minusDays 1
    }

    tradeZoned.withZoneSameInstant(ZoneOffset.UTC).toInstant
  }

  def toTrade(trade: List[String])(implicit timeCollected: Instant) =
    Trade(
      trade(1).toDouble,
      trade(2).toDouble,
      adjustTimestamp(timeCollected, trade(3)),
      timeCollected,
      Some(trade(4)),
      Some(trade(0).toLong)
    )

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

    case (t: Instant, JObject(JField("channel", JString(channel)) :: JField("data", data) :: Nil))
      if preprocess =>
      self forward Data(t, channel, data)

    case Data(t, "ok_sub_spotcny_btc_ticker", data) =>
      implicit val timeCollected = t
      val tick = data.extract[OKCoinStreamingTick]
      sender ! ("ticks", Tick.format.to(tick))

    case Data(t, "ok_sub_spotcny_btc_depth_60", data) =>
      implicit val timeCollected = t
      val ob = data.extract[OKCoinStreamingOrderBook]
      sender ! ("orderbook", OrderBook.format.to(ob))

    case Data(t, "ok_sub_spotcny_btc_trades", data: JArray) =>
      implicit val timeCollected = t
      val trades = data.extract[List[List[String]]] map toTrade
      trades foreach { trade =>
        sender ! ("trades", Trade.format.to(trade))
      }
  }
}

class OKCoinStreamingActor extends Actor with ActorLogging {
  implicit val actorSystem = context.system
  implicit val dispatcher = actorSystem.dispatcher

  val topicPrefix = "okcoin.streaming.btcusd."
  val uri = new URI("wss://real.okcoin.cn:10440/websocket/okcoinapi")

  val channels = List(
    ("event" -> "addChannel") ~ ("channel" -> "ok_sub_spotcny_btc_ticker"),
    ("event" -> "addChannel") ~ ("channel" -> "ok_sub_spotcny_btc_depth_60"),
    ("event" -> "addChannel") ~ ("channel" -> "ok_sub_spotcny_btc_trades")
  )
  val messages = List(TextMessage(compact(render(channels))))

  val websocket = new AkkaWebsocket(uri, messages, self)
  val protocol = context.actorOf(Props[OKCoinWebsocketProtocol])
  websocket.connect

  val pingEvent = ("event" -> "ping")
  actorSystem.scheduler.schedule(30 seconds, 30 seconds) {
    val msg = TextMessage(compact(render(pingEvent)))
    websocket.send(msg)
  }

  def receive = {
    case (topic: String, value: Object) =>
      Producer.send(topicPrefix + topic, value)
    case (t: Instant, msg: String) =>
      val exchange = "okcoin"
      val key = ProducerKey(Producer.uuid, exchange)
      val event = ExchangeEvent(t, Producer.uuid, exchange, msg)
      Producer.send("streaming.websocket.raw", ProducerKey.format.to(key), ExchangeEvent.format.to(event))

      protocol ! (t, parse(msg))
  }
}
