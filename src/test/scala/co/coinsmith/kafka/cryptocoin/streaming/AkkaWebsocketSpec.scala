package co.coinsmith.kafka.cryptocoin.streaming

import java.net.URI
import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.server.Directives
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import akka.testkit.{TestKit, TestProbe}
import org.json4s.JsonAST.JObject
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}


class AkkaWebsocketSpec extends TestKit(ActorSystem("AkkaWebsocketSpecSystem"))
  with FlatSpecLike with BeforeAndAfterAll {
  implicit val materializer = ActorMaterializer()

  import Directives._

  val testWebSocketService =
  Flow[Message]
    .collect {
      case m: TextMessage => m
    }

  val route =
    path("test") {
      get {
        handleWebSocketMessages(testWebSocketService)
      }
    }

  val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)

  override def afterAll {
    TestKit.shutdownActorSystem(system)

    import system.dispatcher // for the future transformations
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }

  "AkkaWebsocket" should "send timestamp and message to receiver" in {
    val probe = TestProbe()
    val messages = List(TextMessage("{}"))
    val websocket = new AkkaWebsocket(new URI("ws://localhost:8080/test"), messages, probe.ref)

    probe.expectMsgPF() {
      case (t: Instant, JObject(Nil)) =>
    }
  }
}
