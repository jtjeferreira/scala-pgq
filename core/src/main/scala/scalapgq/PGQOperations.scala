package scalapgq

import org.joda.time.{DateTime,Duration}
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}

trait PGQConsumerOperations {
  def registerConsumer(queueName: String, consumerName: String)(implicit ec: ExecutionContext): Future[Boolean]
  def unRegisterConsumer(queueName: String, consumerName: String)(implicit ec: ExecutionContext): Future[Boolean]
  
  def getNextBatchEvents(queueName: String, consumerName: String)(implicit ec: ExecutionContext): Future[Option[(Long,Iterable[Event])]]
  def finishBatch(batchId: Long)(implicit ec: ExecutionContext): Future[Boolean]
}

trait PGQOperations {
  
  def createQueue(queueName: String)(implicit ec: ExecutionContext): Future[Boolean]
  def dropQueue(queueName: String, force: Boolean = false)(implicit ec: ExecutionContext): Future[Boolean]
  
  def registerConsumer(queueName: String, consumerName: String)(implicit ec: ExecutionContext): Future[Boolean]
  def unRegisterConsumer(queueName: String, consumerName: String)(implicit ec: ExecutionContext): Future[Boolean]
  
  def insertEvent(queueName: String, eventType: String, eventData: String)(implicit ec: ExecutionContext): Future[Long] = {
    insertEvent(queueName, eventType, eventData, null, null, null, null)
  }
  def insertEvent(queueName: String, eventType: String, eventData: String, extra1: String, extra2: String, extra3: String, extra4: String)(implicit ec: ExecutionContext): Future[Long]
  def insertEventsTransactionally(queueName: String, eventType: String, eventsData: Seq[String])(implicit ec: ExecutionContext): Future[Seq[Long]]
  
  def getQueueInfo()(implicit ec: ExecutionContext): Future[Seq[QueueInfo]]
  def getQueueInfo(queueName: String)(implicit ec: ExecutionContext): Future[Option[QueueInfo]]
  def getConsumerInfo()(implicit ec: ExecutionContext): Future[Seq[ConsumerInfo]]
  def getConsumerInfo(queueName: String)(implicit ec: ExecutionContext): Future[Seq[ConsumerInfo]]
  def getConsumerInfo(queueName: String, consumerName: String)(implicit ec: ExecutionContext): Future[Option[ConsumerInfo]]
  
}