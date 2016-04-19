package scalapgq

import org.scalatest._
import org.scalatest.concurrent._
import akka.actor._
import akka.stream._
import akka.stream.scaladsl._
import akka.testkit._
import akka.stream.testkit.scaladsl._
import scala.concurrent.duration._
import scala.concurrent.Future

abstract class PGQConsumerSpec() extends TestKit(ActorSystem("IntegrationSpec")) with WordSpecLike with PGQSpec with Matchers with BeforeAndAfter with Eventually with IntegrationPatience with ScalaFutures {
  
  implicit val mat = ActorMaterializer()(system)
  implicit val ec = system.dispatcher
  
  def PGQ: PGQ
  def ops: PGQOperations
  
  def withQueue(testCode: String => Any) {
    import java.util.UUID.randomUUID
    val queueName = s"test_queue_${randomUUID()}"
    ops.createQueue(queueName).futureValue
    try{
      testCode(queueName)
    } finally {
      ops.dropQueue(queueName, force = true).futureValue
    }
  }
  
  def withQueueWithNElements(n: Int)(testCode: (String , String) => Any) {
    withQueue { queueName =>
      import java.util.UUID.randomUUID
      val consumerName = s"test_consumer_${randomUUID()}"
      ops.registerConsumer(queueName, consumerName).futureValue
      
      ops.insertEventsTransactionally(queueName,"eventType", (1 to n).map(i => s"eventData_$i")).futureValue
      testCode(queueName, consumerName)
    }
  }
  
  "PGQ streams" should {
    "Try to consume events but error" in {
      PGQ.source(PGQSettings(PostgresUrl, "wronguser", "wronpass", "queue", "consumer"))
        .runWith(TestSink.probe[Event])
        .request(1)
        .expectError().getMessage should include("role \"wronguser\" does not exist")
    }
    
    "Try to consume events from empty queue" in withQueue { queueName => 
      val consumerName = "test_consumer"
      val (switch, downstream) = PGQ.source(PGQSettings(PostgresUrl, PostgresUser, PostgresPassword, queueName, consumerName, registerConsumer = true))
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(TestSink.probe)(Keep.both).run()
        
      downstream
        .request(1)
        .expectNoMsg()
        
      switch.shutdown()

      downstream.expectComplete()
    }
    
    "Consume events already in queue" in withQueueWithNElements(5) { (queueName, consumerName) => 
      val (switch, downstream) = PGQ.source(PGQSettings(PostgresUrl, PostgresUser, PostgresPassword, queueName, consumerName))
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(TestSink.probe)(Keep.both).run()
      
      downstream
        .request(4)
        .receiveWithin(6 seconds, 4)
      downstream
        .request(1)
        .expectNext()
        
      switch.shutdown()
      downstream.expectComplete()
    }
    
    "Consume new events" in withQueueWithNElements(0) { (queueName, consumerName) =>
      val (switch, downstream) = PGQ.source(PGQSettings(PostgresUrl, PostgresUser, PostgresPassword, queueName, consumerName))
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(TestSink.probe)(Keep.both).run()      

      downstream
        .request(1)
        .expectNoMsg()
        
      ops.insertEvent(queueName, "eventType", "eventData").futureValue
      downstream.request(1).receiveWithin(6 seconds, 1)

      switch.shutdown()
      downstream.expectComplete()
    }
    
    "Allow several materializations" in withQueueWithNElements(2) { (queueName, consumerName) => 
      val source = PGQ.source(PGQSettings(PostgresUrl, PostgresUser, PostgresPassword, queueName, consumerName))
      val (switch1, downstream1) = source.viaMat(KillSwitches.single)(Keep.right)
        .toMat(TestSink.probe)(Keep.both).run()
      
      val ev1 = downstream1.request(1).receiveWithin(6 seconds, 1)
      switch1.shutdown()
      downstream1.expectComplete()
      
      println("Completed")
      
      val (switch2, downstream2) = source.viaMat(KillSwitches.single)(Keep.right)
        .toMat(TestSink.probe)(Keep.both).run()
      val ev2 = downstream2.request(1).receiveWithin(6 seconds, 1)
      switch2.shutdown()
      downstream2.expectComplete()
      
      //Since the first stream didn't consume all the events the batch wasn't finished. 
      //Hence the second stream will consume the same batch
      ev1 shouldEqual ev2
    }
  }
}