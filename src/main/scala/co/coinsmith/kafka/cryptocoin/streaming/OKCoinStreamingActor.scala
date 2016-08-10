package co.coinsmith.kafka.cryptocoin.streaming

import java.net.URI
import java.time._
import javax.websocket.MessageHandler.Whole
import javax.websocket.{ClientEndpointConfig, Endpoint, EndpointConfig, Session}

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import co.coinsmith.kafka.cryptocoin.producer.ProducerBehavior
import org.glassfish.tyrus.client.ClientManager
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._

case class Data(timeCollected: Instant, channel: String, data: JValue)

class OKCoinWebsocket extends Actor with ActorLogging {
  var receiver : ActorRef = _

  val uri = new URI("wss://real.okcoin.cn:10440/websocket/okcoinapi")

  val channels = List(
    ("event" -> "addChannel") ~ ("channel" -> "ok_sub_spotcny_btc_ticker"),
    ("event" -> "addChannel") ~ ("channel" -> "ok_sub_spotcny_btc_depth_60"),
    ("event" -> "addChannel") ~ ("channel" -> "ok_sub_spotcny_btc_trades")
  )
  val msg = compact(render(channels))

  val cec = ClientEndpointConfig.Builder.create().build
  val client = ClientManager.createClient
  val endpoint = new Endpoint {
    override def onOpen(session: Session, config: EndpointConfig) = {
      try {
        // setup message handler and subscribe to channels
        session.addMessageHandler(new Whole[String] {
          override def onMessage(message: String) {
            val timeCollected = Instant.now
            log.debug("Received message {} at time {}", message, timeCollected.toString)
            receiver ! (timeCollected, parse(message))
          }
        })

        session.getBasicRemote.sendText(msg)
        log.debug("Sent initialization message: {}", msg)
      } catch {
        case ex: Exception => throw ex
      }
    }
  }

  def connect = {
    client.connectToServer(endpoint, cec, uri)
    log.info("Websocket connected.")
  }

  def receive = {
    case actor: ActorRef => receiver = actor
    case Connect => connect
  }
}

class OKCoinWebsocketProtocol extends Actor with ActorLogging {
  implicit val formats = DefaultFormats

  def mergeInstant(key: String, t: Instant, json: JValue) = {
    render(key -> t.toString) merge json
  }

  def receive = {
    case (t: Instant, events: JArray) =>
      // OKCoin websocket responses are an array of multiple events
      events transformField {
        case JField("timestamp", JString(t)) => ("timestamp" -> Instant.ofEpochMilli(t.toLong).toString)
      } match {
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
      val json = data.transformField {
        case JField("sell", v) => JField("ask", v)
        case JField("buy", v) => JField("bid", v)
        case JField("vol", JString(v)) => JField("volume", JDecimal(BigDecimal(v.replace(",", ""))))
        case JField(key, JString(value)) if key != "timestamp" => JField(key, JDecimal(BigDecimal(value)))
      } merge render("time_collected" -> t.toString)
      sender ! ("ticks", json)

    case Data(t, "ok_sub_spotcny_btc_depth_60", data) =>
      sender ! ("orderbook", mergeInstant("time_collected", t, data))

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

class OKCoinStreamingActor extends Actor with ProducerBehavior {
  val topicPrefix = "okcoin.streaming.btcusd."

  val websocket = context.actorOf(Props[OKCoinWebsocket])
  val protocol = context.actorOf(Props[OKCoinWebsocketProtocol])

  override def preStart = {
    websocket ! self
    websocket ! Connect
  }

  def receive = producerBehavior orElse {
    case (t: Instant, json: JValue) => protocol ! (t, json)
  }
}