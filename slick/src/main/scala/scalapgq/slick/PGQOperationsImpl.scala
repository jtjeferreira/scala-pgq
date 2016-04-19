package scalapgq.slick

import scala.language.implicitConversions
import scalapgq._
import org.joda.time.{DateTime, Duration, Period}
import scala.util.{Failure, Success}
import scala.concurrent.{ExecutionContext, Future}
import _root_.slick.driver.PostgresDriver
import _root_.slick.driver.PostgresDriver.api.Session
import _root_.slick.jdbc._
import com.github.tminglei.slickpg.PgDateSupportJoda

object MyPostgresDriver extends PostgresDriver with PgDateSupportJoda {
  override val api = new API with JodaDateTimePlainImplicits
}

class PGQConsumerOperationsImpl(url: String, user: String, password: String) extends PGQConsumerOperations {
  import MyPostgresDriver.api._
  val db: Database = Database.forURL(url, user, password, executor = AsyncExecutor("PGQ", numThreads = 1, queueSize = 1))
  
  private implicit def run[A](a: DBIOAction[A, NoStream, _])(implicit ec: ExecutionContext): Future[A] = {
    db.run(a)
  }
  
  override def registerConsumer(queueName: String, consumerName: String)(implicit ec: ExecutionContext): Future[Boolean] = {
    sql"select pgq.register_consumer(${queueName}, ${consumerName})".as[Boolean].headOption.map(_.getOrElse(false))
  }
  override def unRegisterConsumer(queueName: String, consumerName: String)(implicit ec: ExecutionContext): Future[Boolean] = {
    sql"select pgq.unregister_consumer(${queueName}, ${consumerName})".as[Boolean].headOption.map(_.getOrElse(false))
  }

  override def getNextBatchEvents(queueName: String,consumerName: String)(implicit ec: ExecutionContext): Future[Option[(Long, Iterable[Event])]] = {
    val a = for {
      batchOption: Option[Long] <- nextBatch(queueName, consumerName)
      r <- batchOption match {
        case Some(batchId) => getBatchEvents(batchId).map(evs => Some(batchId -> evs))
        case None => DBIO.successful(None)
      }
    } yield r
    a.transactionally
  }
  
  private def nextBatch(queueName: String, consumerName: String)(implicit ec: ExecutionContext) = {
    sql"select next_batch from pgq.next_batch(${queueName}, ${consumerName})".as[Option[Long]].headOption.map(_.flatten)
  }
  private def getBatchEvents(batchId: Long)(implicit ec: ExecutionContext) = {
    sql"select * from pgq.get_batch_events(${batchId})".as[Event]
  }
  override def finishBatch(batchId: Long)(implicit ec: ExecutionContext): Future[Boolean] = {
    sql"select pgq.finish_batch(${batchId})".as[Boolean].headOption.map(_.getOrElse(false))
  }
  
  implicit val getEvent = GetResult(r => Event(r.<<, r.<<, r.<<, r.<<, r.<<,r.<<, r.<<, r.<<, r.<<, r.<<))
}

class PGQOperationsImpl(url: String, user: String, password: String) extends PGQOperations {
  import MyPostgresDriver.api._
  
  val db: Database = Database.forURL(url, user, password, executor = AsyncExecutor("PGQ", numThreads = 1, queueSize = 1))
  
  private implicit def run[A](a: DBIOAction[A, NoStream, _])(implicit ec: ExecutionContext): Future[A] = {
    db.run(a)
  }
  
  override def createQueue(queueName: String)(implicit ec: ExecutionContext): Future[Boolean] = {
    sql"select pgq.create_queue(${queueName})".as[Boolean].headOption.map(_.getOrElse(false))
  }
  override def dropQueue(queueName: String, force: Boolean = false)(implicit ec: ExecutionContext): Future[Boolean] = {
    sql"select pgq.drop_queue(${queueName}, ${force})".as[Boolean].headOption.map(_.getOrElse(false))
  }

  override def registerConsumer(queueName: String, consumerName: String)(implicit ec: ExecutionContext): Future[Boolean] = {
    sql"select pgq.register_consumer(${queueName}, ${consumerName})".as[Boolean].headOption.map(_.getOrElse(false))
  }
  override def unRegisterConsumer(queueName: String, consumerName: String)(implicit ec: ExecutionContext): Future[Boolean] = {
    sql"select pgq.unregister_consumer(${queueName}, ${consumerName})".as[Boolean].headOption.map(_.getOrElse(false))
  }
  
  override def getQueueInfo()(implicit ec: ExecutionContext) = {
    sql"select * from pgq.get_queue_info()".as[QueueInfo]
  }
  override def getQueueInfo(queueName: String)(implicit ec: ExecutionContext): Future[Option[QueueInfo]] = {
    sql"select * from pgq.get_queue_info(${queueName})".as[QueueInfo].headOption
  }
  
  override def getConsumerInfo()(implicit ec: ExecutionContext): Future[Seq[ConsumerInfo]] = {
    sql"select * from pgq.get_consumer_info()".as[ConsumerInfo]
  }
  override def getConsumerInfo(queueName: String)(implicit ec: ExecutionContext): Future[Seq[ConsumerInfo]] = {
    sql"select * from pgq.get_consumer_info(${queueName})".as[ConsumerInfo]
  }
  override def getConsumerInfo(queueName: String, consumerName: String)(implicit ec: ExecutionContext): Future[Option[ConsumerInfo]] = {
    sql"select * from pgq.get_consumer_info(${queueName}, ${consumerName})".as[ConsumerInfo].headOption
  }
  
  override def insertEvent(queueName: String, eventType: String, eventData: String, extra1: String = null, extra2: String = null, extra3: String = null, extra4: String = null)(implicit ec: ExecutionContext): Future[Long] = {
    sql"select pgq.insert_event(${queueName}, ${eventType} , ${eventData}, ${extra1}, ${extra2}, ${extra3}, ${extra4})".as[Long].headOption.map(_.get)
  }
  
  override def insertEventsTransactionally(queueName: String, eventType: String, eventsData: Seq[String])(implicit ec: ExecutionContext): Future[Seq[Long]] = {
    DBIO.sequence(eventsData.map { eventData =>
      sql"select pgq.insert_event(${queueName}, ${eventType} , ${eventData})".as[Long].headOption.map(_.get)
    }).transactionally
  }

  implicit val getQueueInfo = GetResult(r => QueueInfo(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<,r.<<))
  implicit val getConsumerInfo = GetResult(r => ConsumerInfo(r.<<, r.<<, r.<<, r.<<, r.<<,r.<<, r.<<, r.<<))
}