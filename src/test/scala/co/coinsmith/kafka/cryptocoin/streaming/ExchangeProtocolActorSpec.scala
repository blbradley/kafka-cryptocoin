package co.coinsmith.kafka.cryptocoin.streaming

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}


abstract class ExchangeProtocolActorSpec(_system: ActorSystem) extends TestKit(_system)
  with FlatSpecLike with BeforeAndAfterAll with ImplicitSender {
  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }
}
