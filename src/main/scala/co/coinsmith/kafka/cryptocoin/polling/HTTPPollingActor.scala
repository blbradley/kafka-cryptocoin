package co.coinsmith.kafka.cryptocoin.polling

import scala.concurrent.Future
import scala.concurrent.duration._
import java.time.Instant

import akka.actor.{Actor, ActorLogging}
import akka.http.scaladsl.model.{HttpResponse, ResponseEntity, StatusCodes}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import akka.util.ByteString
import co.coinsmith.kafka.cryptocoin.ExchangeEvent
import co.coinsmith.kafka.cryptocoin.producer.KafkaCryptocoinProducer
import com.typesafe.config.ConfigFactory
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._


abstract class HTTPPollingActor extends Actor with ActorLogging {
  val exchange: String

  implicit val formats = DefaultFormats
  implicit val materializer = ActorMaterializer()
  import context.system
  import context.dispatcher

  val conf = ConfigFactory.load
  val preprocess = conf.getBoolean("kafka.cryptocoin.preprocess")

  def responseEntityToString(entity: ResponseEntity): Future[String] =
    entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(_.utf8String)
  
  def convertFlow[T : Manifest] = Flow[ExchangeEvent].map { e =>
    val json = parse(e.data)
    (e.timestamp, json.extract[T])
  }

  val periodicBehavior : Receive = {
    case "start" =>
      context.system.scheduler.schedule(2 seconds, 30 seconds, self, "tick")
      context.system.scheduler.schedule(2 seconds, 30 seconds, self, "orderbook")
  }

  val responseBehavior : Receive = {
    case HttpResponse(StatusCodes.OK, headers, entity, _) =>
      val timeCollected = Instant.now
      responseEntityToString(entity).foreach { data =>
        val event = ExchangeEvent(timeCollected, KafkaCryptocoinProducer.uuid, exchange, data)
        KafkaCryptocoinProducer.send("polling.raw", exchange, ExchangeEvent.format.to(event))
      }
    case response @ HttpResponse(code, _, _, _) =>
      throw new Exception(s"Request failed with response ${response}")
      response.discardEntityBytes()
  }
}
