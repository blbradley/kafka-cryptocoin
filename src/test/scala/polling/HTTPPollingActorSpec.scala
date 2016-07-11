package polling

import akka.actor.ActorSystem
import akka.testkit.TestKit
import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}


class HTTPPollingActorSpec(_system: ActorSystem) extends TestKit(_system)
  with FlatSpecLike with BeforeAndAfterAll with EmbeddedKafka {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  def fixture(file: String): String = {
    import scala.io.Source
    Source.fromURL(getClass.getResource("/data/" + file)).mkString.stripLineEnd
  }
}
