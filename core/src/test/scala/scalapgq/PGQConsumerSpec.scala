package scalapgq

import org.scalatest._
import org.scalatest.concurrent._
import akka.actor._
import akka.stream._
import akka.stream.scaladsl._
import akka.testkit._
import akka.stream.testkit.scaladsl._
import scala.concurrent._
import org.joda.time.DateTime


class PGQConsumerSpec extends TestKit(ActorSystem("IntegrationSpec")) with WordSpecLike with PGQSpec with Matchers {
  implicit val mat = ActorMaterializer()(system)
  implicit val ec = system.dispatcher
  
  "PGQ Source" should {
    "Wait for registration to complete" in {
      val event = Event.apply(1, DateTime.now, 1, Some(1), Some("ev_type"), Some("ev_data"), None, None, None, None)
      val ops = new MockPGQConsumerOperations(registrationP = Promise(), nextBatchEventsP = Promise.successful(Some(1l -> Seq(event).toIterable)))
      val downstream = new PGQ(s => ops).source(PGQSettings(PostgresUrl, PostgresUser, PostgresPassword, "queueName", "consumerName", registerConsumer = true))
        .runWith(TestSink.probe[Event])
        
      downstream.request(1).expectNoMsg()
      
      ops.registrationP.success(true)
        
      downstream.expectNext()
    }
  }
}

case class MockPGQConsumerOperations(
  registrationP: Promise[Boolean] = Promise.successful(true),
  unregistrationP: Promise[Boolean] = Promise.successful(true),
  nextBatchEventsP: Promise[Option[(Long, Iterable[Event])]] = Promise.successful(None),
  finishBatchP: Promise[Boolean] = Promise.successful(true)
) extends PGQConsumerOperations {
  def registerConsumer(queueName: String,consumerName: String)(implicit ec: ExecutionContext): Future[Boolean] = registrationP.future
  def unRegisterConsumer(queueName: String,consumerName: String)(implicit ec: ExecutionContext): Future[Boolean] = unregistrationP.future
  
  def getNextBatchEvents(queueName: String,consumerName: String)(implicit ec: ExecutionContext): Future[Option[(Long, Iterable[Event])]] = nextBatchEventsP.future
  def finishBatch(batchId: Long)(implicit ec: ExecutionContext): Future[Boolean] = finishBatchP.future
}