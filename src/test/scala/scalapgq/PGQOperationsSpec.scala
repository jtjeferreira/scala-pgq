package scalapgq.scalalike

import scalapgq._
import org.scalatest._
import org.scalatest.concurrent._
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scalikejdbc._

class PGQOperationsSpec extends WordSpec with PGQSpec with Matchers with BeforeAndAfter with Eventually with IntegrationPatience with ScalaFutures {
  
  val pgq = new PGQOperationsImpl(PostgresUrl, PostgresUser, PostgresPassword)
  val queueName = "test_queue"
  val consumerName = "test_consumer"
  
  final def getNextNonEmptyEvents(queueName: String, consumerName: String)(implicit session: pgq.Session): Iterable[Event] = {
    eventually {
      val batch_id = pgq.nextBatch(queueName, consumerName)
  	  batch_id flatMap {
  	    case Some(id) => {
  	      pgq.getBatchEvents(id).flatMap{ events =>
  	        pgq.finishBatch(id)
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
    
//  if(pgq.getConsumerInfo(queueName, consumerName).futureValue.isDefined) {
//          pgq.unRegisterConsumer(queueName, consumerName).futureValue
//        }
//        pgq.dropQueue(queueName).futureValue
  
  before {
    pgq.localAsyncTx {implicit session => 
      pgq.getQueueInfo(queueName).flatMap { 
        case Some(q) => 
          pgq.getConsumerInfo(queueName, consumerName).flatMap {
            case Some(c) => pgq.unRegisterConsumer(queueName, consumerName)
            case None => Future.successful(())
          }.flatMap { _ =>
            pgq.dropQueue(queueName)
          }
        case None => Future.successful(())
      }
    } futureValue
  }
  "PGQ" should {
    "create_queue and drop queue" in {
      pgq.localAsyncTx { implicit s =>
        pgq.createQueue(queueName)
      }.futureValue shouldEqual true
      
      pgq.localAsyncTx { implicit s =>
        pgq.dropQueue(queueName)
      }.futureValue shouldEqual true
    }
    "register and unregister consumer" in {
      pgq.localAsyncTx { implicit session =>
        for {
          _ <- pgq.createQueue(queueName)
          _ <- pgq.registerConsumer(queueName, consumerName)
        } yield ()
      }.futureValue
      
      pgq.localAsyncTx { implicit session =>
        pgq.unRegisterConsumer(queueName, consumerName)
      }.futureValue shouldEqual true
    }
    "insert and consume event" in {
      val consumerName = "test_consumer"
      pgq.localAsyncTx { implicit session =>
        for {
        _ <- pgq.createQueue(queueName)
        _ <- pgq.registerConsumer(queueName, consumerName)
        } yield ()
      }.futureValue
      
      pgq.localAsyncTx{implicit session =>
        pgq.insertEvent(queueName, "type", "data")
      }.futureValue
      
      pgq.localAsyncTx{implicit session =>
        Future { getNextNonEmptyEvents(queueName, consumerName) }
      }.futureValue should not be empty
    }
    
    
    "rollback inserted events" in {
      pgq.localAsyncTx{implicit session =>
        for {
          _ <- pgq.createQueue(queueName)
          _ <- pgq.registerConsumer(queueName, consumerName)
        } yield ()
      }.futureValue

	    a[Exception] should be thrownBy {
	      pgq.localAsyncTx { implicit session =>
	        for {
	          _ <- pgq.insertEvent(queueName, "type", "data1")
	          _ <- Future.failed(new Exception())
	        } yield ()
	      }.futureValue
	    }
	    
	    pgq.localAsyncTx { implicit session =>
        pgq.insertEvent(queueName, "type", "data2")
      }.futureValue
      
      val events = pgq.localAsyncTx { implicit session =>
        Future { getNextNonEmptyEvents(queueName, consumerName) }
      }.futureValue.map(_.ev_data) 
      
      events should not contain (Some("data1"))
      events should contain (Some("data2"))  
    }
  }
}