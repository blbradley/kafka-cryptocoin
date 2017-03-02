package co.coinsmith.kafka.cryptocoin.polling

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import java.time.Instant

import akka.actor.{Actor, ActorLogging}
import akka.http.scaladsl.Http.HostConnectionPool
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, ResponseEntity}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import co.coinsmith.kafka.cryptocoin.ExchangeEvent
import co.coinsmith.kafka.cryptocoin.producer.Producer
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._


abstract class HTTPPollingActor extends Actor with ActorLogging {
  val exchange: String
  val pool: Flow[(HttpRequest, String), (Try[HttpResponse], String), HostConnectionPool]

  implicit val formats = DefaultFormats
  implicit val materializer = ActorMaterializer()
  import context.dispatcher

  val responseFlow = Flow[(Try[HttpResponse], String)].mapAsync(1) {
    case (Success(HttpResponse(statusCode, headers, entity, _)), _) =>
      val timeCollected = Instant.now
      log.debug("Request returned status code {} with entity {}",  statusCode, entity)
      val msg = responseEntityToString(entity)
      msg.map(s => ExchangeEvent(timeCollected, exchange, s))
    case (Failure(response), _) =>
      throw new Exception(s"Request failed with response ${response}")
  }

  val rawSink = Sink.foreach[ExchangeEvent] { event =>
    Producer.send("polling.raw", exchange, ExchangeEvent.format.to(event))
  }

  def responseEntityToString(entity: ResponseEntity): Future[String] =
    entity.toStrict(500 millis) map { _.data.utf8String }
  
  def convertFlow[T : Manifest] = Flow[ExchangeEvent].map { e =>
    val json = parse(e.data)
    (e.timestamp, json.extract[T])
  }

  val selfSink = Sink.actorRef(self, None)

  def request(uri: String) =
    Source.single(HttpRequest(uri = uri) -> "")
      .via(pool)
      .via(responseFlow)
      .alsoTo(rawSink)

  val periodicBehavior : Receive = {
    case "start" =>
      context.system.scheduler.schedule(2 seconds, 30 seconds, self, "tick")
      context.system.scheduler.schedule(2 seconds, 30 seconds, self, "orderbook")
  }
}
