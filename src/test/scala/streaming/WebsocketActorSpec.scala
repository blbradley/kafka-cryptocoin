package streaming

import java.time.Instant
import javax.websocket.server.ServerEndpoint
import javax.websocket.{OnMessage, Session}

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestKitBase, TestProbe}
import co.coinsmith.kafka.cryptocoin.streaming.{Connect, WebsocketActor}
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

class WebsocketActorSpec extends TestContainer with TestKitBase with FlatSpecLike {
  implicit lazy val system = ActorSystem("WebsocketActorSpecSystem")

  val serverEndpoint = new EchoEndpoint
  val uri = getURI(serverEndpoint.getClass)

  "WebsocketActor" should "forward received messages to receiver" in {
    val server = startServer(serverEndpoint.getClass)
    val actorRef = TestActorRef(WebsocketActor.props(uri))
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
