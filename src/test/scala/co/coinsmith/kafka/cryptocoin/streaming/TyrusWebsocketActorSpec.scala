package co.coinsmith.kafka.cryptocoin.streaming

import java.time.Instant
import javax.websocket.server.ServerEndpoint
import javax.websocket.{OnMessage, Session}

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestKitBase, TestProbe}
import org.glassfish.tyrus.test.tools.TestContainer
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._
import org.scalatest.FlatSpecLike

@ServerEndpoint("/WebsocketActorSpec/echoEndpoint")
class EchoEndpoint {
  @OnMessage
  def onMessage(message: String, session: Session) = message
}

class TyrusWebsocketActorSpec extends TestContainer with TestKitBase with FlatSpecLike {
  implicit lazy val system = ActorSystem("TyrusWebsocketActorSpecSystem")

  val serverEndpoint = new EchoEndpoint
  val uri = getURI(serverEndpoint.getClass)

  "TyrusWebsocketActor" should "forward received messages to receiver" in {
    val server = startServer(serverEndpoint.getClass)
    val actorRef = TestActorRef(TyrusWebsocketActor.props(uri))
    val probe = TestProbe("probe")

    val json = List.empty
    val expected = JObject(json)

    actorRef ! probe.ref
    actorRef ! Connect
    actorRef ! compact(render(json))
    probe.expectMsgPF() {
      case (_: Instant, expected) => true
      case _ => false
    }

    stopServer(server)
  }
}
