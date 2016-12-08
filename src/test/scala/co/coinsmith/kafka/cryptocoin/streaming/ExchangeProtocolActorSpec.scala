package co.coinsmith.kafka.cryptocoin.streaming

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}


abstract class ExchangeProtocolActorSpec(_system: ActorSystem) extends TestKit(_system)
  with FlatSpecLike with BeforeAndAfterAll with Matchers with ImplicitSender {
  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }
}
