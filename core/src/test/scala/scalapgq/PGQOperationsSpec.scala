package scalapgq

import scalapgq._
import org.scalatest._
import org.scalatest.concurrent._
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

abstract class PGQOperationsSpec[S] extends WordSpec with PGQSpec with Matchers with BeforeAndAfter with Eventually with IntegrationPatience with ScalaFutures {
  
  val ops: PGQOperations[S]
  val queueName = "test_queue"
  val consumerName = "test_consumer"
  
  final def getNextNonEmptyEvents(queueName: String, consumerName: String)(implicit session: S): Iterable[Event] = {
    eventually {
      val batch_id = ops.nextBatch(queueName, consumerName)
  	  batch_id flatMap {
  	    case Some(id) => {
  	      ops.getBatchEvents(id).flatMap{ events =>
  	        ops.finishBatch(id)
    	      events match {
    	        case Nil => Future.failed(new Exception("Empty batch"))
    	        case s => Future.successful(s) 
    	      }
  	      }
  	    }
  	    case None => Future.failed(new Exception("No batchId"))
  	  } futureValue
    }
  }
    
  before {
    ops.localAsyncTx {implicit session => 
      ops.getQueueInfo(queueName).flatMap { 
        case Some(q) => 
          ops.getConsumerInfo(queueName, consumerName).flatMap {
            case Some(c) => ops.unRegisterConsumer(queueName, consumerName)
            case None => Future.successful(())
          }.flatMap { _ =>
            ops.dropQueue(queueName)
          }
        case None => Future.successful(())
      }
    } futureValue
  }
  "PGQ" should {
    "create_queue and drop queue" in {
      ops.localAsyncTx { implicit s =>
        ops.createQueue(queueName)
      }.futureValue shouldEqual true
      
      ops.localAsyncTx { implicit s =>
        ops.dropQueue(queueName)
      }.futureValue shouldEqual true
    }
    "register and unregister consumer" in {
      ops.localAsyncTx { implicit session =>
        for {
          _ <- ops.createQueue(queueName)
          _ <- ops.registerConsumer(queueName, consumerName)
        } yield ()
      }.futureValue
      
      ops.localAsyncTx { implicit session =>
        ops.unRegisterConsumer(queueName, consumerName)
      }.futureValue shouldEqual true
    }
    "insert and consume event" in {
      val consumerName = "test_consumer"
      ops.localAsyncTx { implicit session =>
        for {
        _ <- ops.createQueue(queueName)
        _ <- ops.registerConsumer(queueName, consumerName)
        } yield ()
      }.futureValue
      
      ops.localAsyncTx{implicit session =>
        ops.insertEvent(queueName, "type", "data")
      }.futureValue
      
      ops.localAsyncTx{implicit session =>
        Future { getNextNonEmptyEvents(queueName, consumerName) }
      }.futureValue should not be empty
    }
    
    
    "rollback inserted events" in {
      ops.localAsyncTx{implicit session =>
        for {
          _ <- ops.createQueue(queueName)
          _ <- ops.registerConsumer(queueName, consumerName)
        } yield ()
      }.futureValue

	    a[Exception] should be thrownBy {
	      ops.localAsyncTx { implicit session =>
	        for {
	          _ <- ops.insertEvent(queueName, "type", "data1")
	          _ <- Future.failed(new Exception())
	        } yield ()
	      }.futureValue
	    }
	    
	    ops.localAsyncTx { implicit session =>
        ops.insertEvent(queueName, "type", "data2")
      }.futureValue
      
      val events = ops.localAsyncTx { implicit session =>
        Future { getNextNonEmptyEvents(queueName, consumerName) }
      }.futureValue.map(_.ev_data) 
      
      events should not contain (Some("data1"))
      events should contain (Some("data2"))  
    }
  }
}