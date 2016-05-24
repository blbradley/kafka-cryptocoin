package streaming

import akka.actor.ActorSystem
import akka.testkit.TestKit
import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}


abstract class ExchangeStreamingActorSpec(_system: ActorSystem) extends TestKit(_system)
  with FlatSpecLike with BeforeAndAfterAll with EmbeddedKafka {
  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }
}
