package scalapgq

import scalapgq._
import org.scalatest._
import org.scalatest.concurrent._
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

abstract class PGQOperationsSpec extends WordSpec with PGQSpec with Matchers with BeforeAndAfter with Eventually with IntegrationPatience with ScalaFutures with OptionValues {
  
  val ops: PGQOperations with PGQConsumerOperations
  val queueName = "test_queue"
  val consumerName = "test_consumer"
  
  final def getNextNonEmptyEvents(queueName: String, consumerName: String): Iterable[Event] = {
    eventually {
      val (batchId, evs) = ops.getNextBatchEvents(queueName, consumerName).futureValue.value
      ops.finishBatch(batchId)
      evs should not be empty
      evs
    }
  }
    
  before {
    ops.getQueueInfo(queueName).flatMap { 
      case Some(q) => 
        ops.getConsumerInfo(queueName, consumerName).flatMap {
          case Some(c) => ops.unRegisterConsumer(queueName, consumerName)
          case None => Future.successful(())
        }.flatMap { _ =>
          ops.dropQueue(queueName)
        }
      case None => Future.successful(())
    } futureValue
  }
  "PGQ" should {
    "create_queue and drop queue" in {
      ops.createQueue(queueName).futureValue shouldEqual true
      
      ops.dropQueue(queueName).futureValue shouldEqual true
    }
    "register and unregister consumer" in {
      (for {
        _ <- ops.createQueue(queueName)
        _ <- ops.registerConsumer(queueName, consumerName)
      } yield ()).futureValue
      
      ops.unRegisterConsumer(queueName, consumerName).futureValue shouldEqual true
    }
    "insert and consume event" in {
      val consumerName = "test_consumer"
      (for {
        _ <- ops.createQueue(queueName)
        _ <- ops.registerConsumer(queueName, consumerName)
      } yield ()).futureValue
      
       ops.insertEvent(queueName, "type", "data").futureValue
      
      getNextNonEmptyEvents(queueName, consumerName) should not be empty
    }
  }
}