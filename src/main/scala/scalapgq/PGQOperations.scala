package scalapgq

import org.joda.time.{DateTime,Duration}
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}

trait PGQOperations {
  type Session
  
  def localAsyncTx[A](execution: Session => Future[A])(implicit ec: ExecutionContext): Future[A]
  
  def createQueue(queueName: String)(implicit s: Session, ec: ExecutionContext): Future[Boolean]
  def dropQueue(queueName: String, force: Boolean = false)(implicit s: Session, ec: ExecutionContext): Future[Boolean]
  
  def registerConsumer(queueName: String, consumerName: String)(implicit s: Session, ec: ExecutionContext): Future[Boolean]
  def unRegisterConsumer(queueName: String, consumerName: String)(implicit s: Session, ec: ExecutionContext): Future[Boolean]
  
  def nextBatch(queueName: String, consumerName: String)(implicit s: Session, ec: ExecutionContext): Future[Option[Long]]
  def getBatchEvents(batchId: Long)(implicit s: Session, ec: ExecutionContext): Future[Iterable[Event]]
  def finishBatch(batchId: Long)(implicit s: Session, ec: ExecutionContext): Future[Boolean]
  def eventRetry(batchId: Long, eventId: Long, retrySeconds: Duration)(implicit s: Session, ec: ExecutionContext): Future[Boolean]
  def eventRetry(batchId: Long, eventId: Long, retryTime: DateTime)(implicit s: Session, ec: ExecutionContext): Future[Boolean]
  def eventFailed(batchId: Long, eventId: Long, reason: String)(implicit s: Session, ec: ExecutionContext): Future[Boolean]
  
  def insertEvent(queueName: String, eventType: String, eventData: String)(implicit s: Session, ec: ExecutionContext): Future[Long] = {
    insertEvent(queueName, eventType, eventData, null, null, null, null)
  }
  def insertEvent(queueName: String, eventType: String, eventData: String, extra1: String, extra2: String, extra3: String, extra4: String)(implicit s: Session, ec: ExecutionContext): Future[Long]
  def getQueueInfo()(implicit s: Session, ec: ExecutionContext): Future[Seq[QueueInfo]]
  def getQueueInfo(queueName: String)(implicit s: Session, ec: ExecutionContext): Future[Option[QueueInfo]]
  def getConsumerInfo()(implicit s: Session, ec: ExecutionContext): Future[Seq[ConsumerInfo]]
  def getConsumerInfo(queueName: String)(implicit s: Session, ec: ExecutionContext): Future[Seq[ConsumerInfo]]
  def getConsumerInfo(queueName: String, consumerName: String)(implicit s: Session, ec: ExecutionContext): Future[Option[ConsumerInfo]]
  
}