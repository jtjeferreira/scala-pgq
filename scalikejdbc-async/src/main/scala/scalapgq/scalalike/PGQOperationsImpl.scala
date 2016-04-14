package scalapgq.scalalike

import scalapgq._
import org.joda.time.{DateTime, Duration, Period}
import scala.concurrent.{ExecutionContext, Future}
import scalikejdbc._
import scalikejdbc.async._
import scalikejdbc.async.FutureImplicits._

class PGQOperationsImpl(url: String, user: String, password: String) extends PGQOperations[TxAsyncDBSession] {
  type Session = TxAsyncDBSession
  
  val acp = AsyncConnectionPoolFactory.apply(url, user, password, AsyncConnectionPoolSettings(maxPoolSize=1, maxQueueSize=1))
  
  override def localAsyncTx[A](execution: Session => Future[A])(implicit ec: ExecutionContext): Future[A] = {
    acp.borrow().toNonSharedConnection()
      .map { nonSharedConnection => TxAsyncDBSession(nonSharedConnection) }
      .flatMap { tx => AsyncTx.inTransaction[A](tx, execution) }
  }
  
  override def createQueue(queueName: String)(implicit s: Session, ec: ExecutionContext): Future[Boolean] = {
    sql"select pgq.create_queue(${queueName})".map(_.boolean(1)).single.future().map(_.getOrElse(false))
  }
  override def dropQueue(queueName: String, force: Boolean = false)(implicit s: Session, ec: ExecutionContext): Future[Boolean] = {
    sql"select pgq.drop_queue(${queueName}, ${force})".map(_.boolean(1)).single.future().map(_.getOrElse(false))
  }

  override def registerConsumer(queueName: String, consumerName: String)(implicit s: Session, ec: ExecutionContext): Future[Boolean] = {
    sql"select pgq.register_consumer(${queueName}, ${consumerName})".map(_.boolean(1)).single.future().map(_.getOrElse(false))
  }
  override def unRegisterConsumer(queueName: String, consumerName: String)(implicit s: Session, ec: ExecutionContext): Future[Boolean] = {
    sql"select pgq.unregister_consumer(${queueName}, ${consumerName})".map(_.boolean(1)).single.future().map(_.getOrElse(false))
  }
  
  override def nextBatch(queueName: String, consumerName: String)(implicit s: Session, ec: ExecutionContext): Future[Option[Long]] = {
    sql"select next_batch from pgq.next_batch(${queueName}, ${consumerName})"
      .map(rs => rs.longOpt("next_batch"))
      .single()
      .future()
      .map(_.flatten)
  }
  def getBatchEvents(batchId: Long)(implicit s: Session, ec: ExecutionContext): Future[Iterable[Event]] = {
    sql"select * from pgq.get_batch_events(${batchId})"
      .map(rs => new Event(
        rs.long("ev_id"),
        rs.jodaDateTime("ev_time"),
        rs.long("ev_txid"),
        rs.intOpt("ev_retry"),
        rs.stringOpt("ev_type"),
        rs.stringOpt("ev_data"),
        rs.stringOpt("ev_extra1"),
        rs.stringOpt("ev_extra2"),
        rs.stringOpt("ev_extra3"),
        rs.stringOpt("ev_extra4")))
      .list
      .future()
  }
  def finishBatch(batchId: Long)(implicit s: Session, ec: ExecutionContext): Future[Boolean] = {
    sql"select pgq.finish_batch(${batchId})".map(_.boolean(1)).single.future().map(_.getOrElse(false))
  }
  
  override def eventRetry(batchId: Long, eventId: Long, retryDuration: Duration)(implicit s: Session, ec: ExecutionContext) = {
    sql"select pgq.event_retry(${batchId}, ${eventId}, ${retryDuration.getStandardSeconds().toInt})".execute.future()
  }

  override def eventRetry(batchId: Long, eventId: Long, retryTime: DateTime)(implicit s: Session, ec: ExecutionContext) = {
    sql"select pgq.event_retry(${batchId}, ${eventId}, ${retryTime})".execute.future()
  }

  override def eventFailed(batchId: Long, eventId: Long, reason: String)(implicit s: Session, ec: ExecutionContext) = {
    sql"select pgq.event_failed(${batchId}, ${eventId}, ${reason})".execute.future()
  }
  
  override def getQueueInfo()(implicit s: Session, ec: ExecutionContext) = {
    sql"select * from pgq.get_queue_info()"
      .map(rs => new QueueInfo(
        rs.string("queue_name"),
        rs.int("queue_ntables"),
        rs.int("queue_cur_table"),
        rs.get("queue_rotation_period"),
        rs.jodaDateTime("queue_switch_time"),
        rs.boolean("queue_external_ticker"),
        rs.boolean("queue_ticker_paused"),
        rs.int("queue_ticker_max_count"),
        rs.get("queue_ticker_max_lag"),
        rs.get("queue_ticker_idle_period"),
        rs.get("ticker_lag"),
        rs.doubleOpt("ev_per_sec"),
        rs.long("ev_new"),
        rs.long("last_tick_id")
      ))
      .list
      .future()
  }
  override def getQueueInfo(queueName: String)(implicit s: Session, ec: ExecutionContext): Future[Option[QueueInfo]] = {
    sql"select * from pgq.get_queue_info(${queueName})"
      .map(rs => new QueueInfo(
        rs.string("queue_name"),
        rs.int("queue_ntables"),
        rs.int("queue_cur_table"),
        rs.get("queue_rotation_period"),
        rs.jodaDateTime("queue_switch_time"),
        rs.boolean("queue_external_ticker"),
        rs.boolean("queue_ticker_paused"),
        rs.int("queue_ticker_max_count"),
        rs.get("queue_ticker_max_lag"),
        rs.get("queue_ticker_idle_period"),
        rs.get("ticker_lag"),
        rs.doubleOpt("ev_per_sec"),
        rs.long("ev_new"),
        rs.long("last_tick_id")
      ))
      .single
      .future()
  }
  
  override def getConsumerInfo()(implicit s: Session, ec: ExecutionContext): Future[Seq[ConsumerInfo]] = {
    sql"select * from pgq.get_consumer_info()"
      .map(rs => new ConsumerInfo(
        rs.string("queue_name"),
        rs.string("consumer_name"),
        rs.get("lag"),
        rs.get("last_seen"),
        rs.long("last_tick"),
        rs.intOpt("current_batch"),
        rs.intOpt("next_tick"),
        rs.long("pending_events")
      ))
      .list
      .future()
  }
  override def getConsumerInfo(queueName: String)(implicit s: Session, ec: ExecutionContext): Future[Seq[ConsumerInfo]] = {
    sql"select * from pgq.get_consumer_info(${queueName})"
      .map(rs => new ConsumerInfo(
        rs.string("queue_name"),
        rs.string("consumer_name"),
        rs.get("lag"),
        rs.get("last_seen"),
        rs.long("last_tick"),
        rs.intOpt("current_batch"),
        rs.intOpt("next_tick"),
        rs.long("pending_events")
      ))
      .list
      .future()
  }
  override def getConsumerInfo(queueName: String, consumerName: String)(implicit s: Session, ec: ExecutionContext): Future[Option[ConsumerInfo]] = {
    sql"select * from pgq.get_consumer_info(${queueName}, ${consumerName})"
      .map(rs => new ConsumerInfo(
        rs.string("queue_name"),
        rs.string("consumer_name"),
        rs.get("lag"),
        rs.get("last_seen"),
        rs.long("last_tick"),
        rs.intOpt("current_batch"),
        rs.intOpt("next_tick"),
        rs.long("pending_events")
      ))
      .first
      .future()
  }
  
  override def insertEvent(queueName: String, eventType: String, eventData: String, extra1: String = null, extra2: String = null, extra3: String = null, extra4: String = null)(implicit s: Session, ec: ExecutionContext): Future[Long] = {
    sql"select pgq.insert_event(${queueName}, ${eventType} , ${eventData}, ${extra1}, ${extra2}, ${extra3}, ${extra4})".map(_.long(1)).single.future().map(_.get)
  }
  
  implicit val array: TypeBinder[Period] = {
    TypeBinder(_ getObject _)(_ getObject _).map{
      case p: org.joda.time.Period => p 
    }
  }
}