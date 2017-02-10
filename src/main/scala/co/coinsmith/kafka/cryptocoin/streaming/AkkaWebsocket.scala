package co.coinsmith.kafka.cryptocoin.streaming

import scala.concurrent.{ExecutionContext, Future, Promise}
import java.net.URI
import java.time.Instant

import akka.Done
import akka.actor.{ActorRef, ActorSystem}
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, Sink, Source, Zip}
import akka.stream.{ActorMaterializer, FlowShape}
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods.parse


class AkkaWebsocket(uri: URI, messages: List[TextMessage], receiver: ActorRef)(implicit system: ActorSystem) {
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()
  val log = Logging(system.eventStream, this.getClass.getName)
  var promise: Promise[Option[Message]] = _

  // emit initial message and then keep the connection open
  val source: Source[Message, Promise[Option[Message]]] =
    Source(messages).concatMat(Source.maybe[Message])(Keep.right)

  val receiverSink = Sink.foreach[(Instant, String)] { receiver ! _ }

  val websocketFlow: Flow[Message, Message, (Future[Done], Promise[Option[Message]])] =
    Flow.fromGraph(GraphDSL.create(receiverSink, source)((_,_)) { implicit b =>
      (sink, source) =>
        import GraphDSL.Implicits._

        val bcast = b.add(Broadcast[Message](2))
        val zip = b.add(Zip[Instant, String]())
        val stringFlow = b.add(Flow[Message].mapAsync(1)(messageToString))

        bcast.out(0) ~> Flow[Message].map(_ => Instant.now) ~> zip.in0
        bcast.out(1) ~> stringFlow                          ~> zip.in1

        zip.out ~> sink

        FlowShape(bcast.in, source.out)
    })

  def connect: Unit = {
    val (upgradeResponse, (sinkClosed, promise)) =
      Http().singleWebSocketRequest(
        WebSocketRequest(uri.toString),
        websocketFlow)

    this.promise = promise
    sinkClosed.onComplete { _ =>
      log.info("Reconnecting in five seconds to {}", uri)
      Thread.sleep(5000)
      connect
    }
  }

  def messageToString(m: Message)(implicit ec: ExecutionContext): Future[String] = m match {
    case TextMessage.Strict(m) => Future(m)
    case TextMessage.Streamed(ms) => ms.runFold("")(_ + _)
    case m => throw new RuntimeException("Received unhandled websocket message type.")
  }

  def disconnect = {
    promise.success(None)
  }
}
