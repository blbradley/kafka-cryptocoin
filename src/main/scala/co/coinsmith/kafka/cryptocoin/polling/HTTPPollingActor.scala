package co.coinsmith.kafka.cryptocoin.polling

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import java.time.Instant

import akka.actor.{Actor, ActorLogging}
import akka.http.scaladsl.Http.HostConnectionPool
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, ResponseEntity}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import co.coinsmith.kafka.cryptocoin.producer.Producer
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._


abstract class HTTPPollingActor extends Actor with ActorLogging {
  val pool: Flow[(HttpRequest, String), (Try[HttpResponse], String), HostConnectionPool]

  implicit val formats = DefaultFormats
  implicit val materializer = ActorMaterializer()
  import context.dispatcher
  val tick = context.system.scheduler.schedule(2 seconds, 30 seconds, self, "tick")
  val orderbook = context.system.scheduler.schedule(2 seconds, 30 seconds, self, "orderbook")

  val responseFlow = Flow[(Try[HttpResponse], String)].map {
    case (Success(HttpResponse(statusCode, _, entity, _)), _) =>
      val timeCollected = Instant.now
      log.debug("Request returned status code {} with entity {}",  statusCode, entity)
      (timeCollected, entity)
    case (Failure(response), _) =>
      throw new Exception(s"Request failed with response ${response}")
  }

  def responseEntityToString(entity: ResponseEntity) = {
    val data = entity.toStrict(500 millis) map { _.data.utf8String }
    Await.result(data, 500 millis)
  }

  val selfSink = Sink.actorRef(self, None)

  def request(uri: String) =
    Source.single(HttpRequest(uri = uri) -> "")
      .via(pool)
      .via(responseFlow)
}
