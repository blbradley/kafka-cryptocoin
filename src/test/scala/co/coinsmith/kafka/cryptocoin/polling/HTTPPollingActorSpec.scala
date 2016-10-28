package co.coinsmith.kafka.cryptocoin.polling

import java.time.Instant

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.ResponseEntity
import akka.stream.scaladsl.{Flow, Keep}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestKit
import org.json4s.JsonAST.JValue
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}


class HTTPPollingActorSpec(_system: ActorSystem) extends TestKit(_system)
  with FlatSpecLike with BeforeAndAfterAll {

  override def afterAll = {
    TestKit.shutdownActorSystem(system)
  }

  def testExchangeFlowPubSub(flow: Flow[(Instant, ResponseEntity), (String, JValue), NotUsed]) =
    TestSource.probe[(Instant, ResponseEntity)]
      .via(flow)
      .toMat(TestSink.probe[(String, JValue)])(Keep.both)
}
