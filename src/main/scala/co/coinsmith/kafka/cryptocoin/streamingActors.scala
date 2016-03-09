package co.coinsmith.kafka.cryptocoin

import scala.concurrent.Future
import scala.math.BigDecimal.RoundingMode
import java.net.URI
import java.time._
import javax.websocket.MessageHandler.Whole
import javax.websocket._

import akka.actor.{Actor, ActorLogging, Props}
import com.xeiam.xchange.Exchange
import com.xeiam.xchange.dto.marketdata.{OrderBook, Ticker, Trade}
import com.xeiam.xchange.service.streaming.{ExchangeEventType, ExchangeStreamingConfiguration}
import org.glassfish.tyrus.client.ClientManager
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._


class StreamingActor extends Actor {
  val okcoin = context.actorOf(Props[OKCoinStreamingActor], "okcoin")
  def receive = {
    case e: Exchange =>
      ExchangeService.getStreamingConfig(e) match {
        case Some(sc) =>
          val name = ExchangeService.getNameFromExchange(e).toLowerCase
          context.actorOf(Props(classOf[ExchangeStreamingActor], e, sc), name)
        case None =>
      }
  }
}

class ExchangeStreamingActor(exchange: Exchange, streamConfig: ExchangeStreamingConfiguration)
  extends Actor {
  val key = exchange.getExchangeSpecification.getExchangeName
  val marketDataService = exchange.getStreamingExchangeService(streamConfig)

  import context.dispatcher
  override def preStart = {
    Future {
      marketDataService.connect
    }.onComplete {
      case _ => getNextEvent
    }
  }

  def getNextEvent = {
    val event = marketDataService.getNextEvent
    val timeCollected = System.currentTimeMillis
    self ! (timeCollected, event.getEventType, event.getPayload)
  }

  def receive = {
    case (_, ExchangeEventType.DISCONNECT, _) =>
      throw new Exception("Received WebSocket disconnect event")

    case (topic: String, key: String, json: JObject) =>
      val msg = compact(render(json))
      context.actorOf(Props[KafkaProducerActor]) ! (topic, key, msg)
      getNextEvent

    case (t: Long, ExchangeEventType.TICKER, tick: Ticker) =>
      val json = Utils.tickerToJson(tick, t)
      self ! ("stream_ticks", key, json)

    case (t: Long, ExchangeEventType.SUBSCRIBE_ORDERS, ob: OrderBook) =>
      val json = Utils.orderBookXChangeToJson(ob, t)
      self ! ("stream_orders", key, json)

    case (t: Long, ExchangeEventType.DEPTH, ob: OrderBook) =>
      val json = Utils.orderBookXChangeToJson(ob, t)
      self ! ("stream_depth", key, json)

    case (t: Long, ExchangeEventType.TRADE, trade: Trade) =>
      val timestamp = Option(trade.getTimestamp).map { _.getTime }
      val orderType = Option(trade.getType).map { _.toString.toLowerCase }
      val price = BigDecimal(trade.getPrice.setScale(2, RoundingMode.HALF_DOWN)
        .bigDecimal.stripTrailingZeros)
      val volume = BigDecimal(trade.getTradableAmount.setScale(8, RoundingMode.HALF_DOWN)
        .bigDecimal.stripTrailingZeros)
      val json = ("timestamp" -> timestamp) ~
        ("time_collected" -> t) ~
        ("id" -> trade.getId) ~
        ("type" -> orderType) ~
        ("currencyPair" -> trade.getCurrencyPair.toString) ~
        ("price" -> price) ~ ("volume" -> volume)
      self ! ("stream_trades", key, json)
  }
}

class OKCoinStreamingActor extends Actor with ActorLogging {
  implicit val formats = DefaultFormats
  val key = "OKCoin"

  val cec = ClientEndpointConfig.Builder.create().build
  val client = ClientManager.createClient
  val endpoint = new Endpoint {
    override def onOpen(session: Session, config: EndpointConfig) = {
      try {
        session.addMessageHandler(new Whole[String] {
          override def onMessage(message: String) {
            val timeCollected = Instant.now
            // OKCoin websocket responses are always wrapped in an array
            val json = parse(message)(0) transformField {
              case JField("timestamp", JString(t)) => ("timestamp" -> t.toLong)
            }
            self ! (timeCollected, json)
          }
        })
        val channels = List(
          ("event" -> "addChannel") ~ ("channel" -> "ok_sub_spotcny_btc_ticker"),
          ("event" -> "addChannel") ~ ("channel" -> "ok_sub_spotcny_btc_depth_60"),
          ("event" -> "addChannel") ~ ("channel" -> "ok_sub_spotcny_btc_trades")
        )
        val msg = compact(render(channels))
        session.getBasicRemote.sendText(msg)
        log.debug("Sent initialization message: {}", msg)
      } catch {
        case ex: Exception => throw ex
      }
    }
  }

  override def preStart = {
    client.connectToServer(endpoint, cec, new URI("wss://real.okcoin.cn:10440/websocket/okcoinapi"))
    log.info("OKCoin Websocket connected.")
  }

  case class Data(timeCollected: Instant, channel: String, data: JValue)

  def receive = {
    case (t, JObject(JField("channel", JString(channel)) ::
                     JField("success", JString(success)) :: Nil)) =>
      log.info("Added channel {} at time {}.", channel, t)

    case (t, JObject(JField("channel", JString(channel)) ::
                   JField("success", JString(success)) ::
                   JField("error_code", JInt(errorCode)) :: Nil)) =>
      log.error("Adding channel {} failed at time {}. Error code {}.", channel, t, errorCode)

    case (t: Instant, JObject(JField("channel", JString(channel)) :: JField("data", data) :: Nil)) =>
      self ! Data(t, channel, data)

    case Data(t, "ok_sub_spotcny_btc_ticker", data) =>
      val json = data.transformField {
        case JField("vol", JString(v)) => JField("volume", JDecimal(BigDecimal(v.replace(",", ""))))
        case JField(key, JString(value)) => JField(key, JDecimal(BigDecimal(value)))
      } merge render("time_collected" -> t.toEpochMilli)
      self ! ("stream_ticks", key, json)

    case Data(t, "ok_sub_spotcny_btc_depth_60", data) =>
      val timestamp = Some((data \ "timestamp").extract[Long])
      val asks = (data \ "asks").extract[List[List[BigDecimal]]]
      val bids = (data \ "bids").extract[List[List[BigDecimal]]]
      val json = Utils.orderBookToJson(timestamp, t.toEpochMilli, asks, bids)
      self ! ("stream_depth", key, json)

    case Data(t, "ok_sub_spotcny_btc_trades", data: JArray) =>
      val json = data.transform {
        case JArray(JString(id) :: JString(p) :: JString(v) :: JString(time) :: JString(kind) :: Nil) =>
          val collectedDateTime = LocalDateTime.ofInstant(t, ZoneId.of("Asia/Shanghai"))
          var tradeDateTime = LocalTime.parse(time).atDate(collectedDateTime.toLocalDate)
          if ((tradeDateTime compareTo collectedDateTime) > 0) {
            // correct date if time collected happens right after midnight
            tradeDateTime = tradeDateTime minusDays 1
          }
          val timestamp = tradeDateTime.toInstant(ZoneOffset.UTC).toEpochMilli
          ("timestamp" -> timestamp) ~
            ("time_collected" -> t.toEpochMilli) ~
            ("id" -> id.toLong) ~
            ("price" -> BigDecimal(p)) ~
            ("volume" -> BigDecimal(v)) ~
            ("type" -> kind)
      }
      self ! ("stream_trades", key, json)

    case (topic: String, key: String, json: JValue) =>
      val msg = compact(render(json))
      context.actorOf(Props[KafkaProducerActor]) ! (topic, key, msg)
  }
}
