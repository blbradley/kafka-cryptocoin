package co.coinsmith.kafka.cryptocoin.streaming

import java.io.File
import java.time.Instant

import akka.actor.{ActorSystem, Props}
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import org.iq80.leveldb.util.FileUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

class TestPersistentStreamingActor extends PersistentStreamingActor {
  override def persistenceId: String = "test-persistent-actor"
}

class PersistentStreamingActorSpec extends TestKit(ActorSystem("PersistentStreamingActorSpecSystem"))
  with FlatSpecLike with BeforeAndAfterAll {
  val config = ConfigFactory.load
  val storageLocations = List(
    new File(system.settings.config.getString("akka.persistence.journal.leveldb.dir")),
    new File(config.getString("akka.persistence.snapshot-store.local.dir"))
  )

  override def beforeAll() {
    storageLocations foreach FileUtils.deleteRecursively
  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
    storageLocations foreach FileUtils.deleteRecursively
  }

  "PersistentStreamingActor" should "publish received message on event stream" in {
    val timestamp = Instant.ofEpochSecond(10L)
    val exchange = "myexchange"
    val msg = ""
    val command = SaveWebsocketMessage(timestamp, exchange, msg)

    val subscriber = TestProbe()
    system.eventStream.subscribe(subscriber.ref, classOf[WebsocketMessage])

    val actor = system.actorOf(Props[TestPersistentStreamingActor])
    actor ! command

    val event = subscriber expectMsgClass classOf[WebsocketMessage]
    assert(event.exchange == exchange)
    assert(event.msg == msg)
  }
}
