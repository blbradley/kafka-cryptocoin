package co.coinsmith.kafka.cryptocoin.polling

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import java.time.Instant

import akka.actor.{Actor, ActorLogging}
import akka.http.scaladsl.model.HttpResponse
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import org.json4s.DefaultFormats


abstract class HTTPPollingActor extends Actor with ActorLogging {
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
}
