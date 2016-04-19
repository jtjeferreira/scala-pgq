package scalapgq.scalalike

import scalapgq._
import org.joda.time.{DateTime, Duration, Period}
import scala.concurrent.{ExecutionContext, Future}
import scalikejdbc._

class PGQOperationsImpl(url: String, user: String, password: String) extends PGQOperations with PGQConsumerOperations {
  val cp = ConnectionPool.DEFAULT_CONNECTION_POOL_FACTORY.apply(url, user, password, ConnectionPoolSettings(initialSize = 1, maxSize=1))
  
  private def localAsyncTx[A](execution: DBSession => A)(implicit ec: ExecutionContext): Future[A] = Future { using(DB(cp.borrow())){db => db.localTx { execution }} }
  
  override def createQueue(queueName: String)(implicit ec: ExecutionContext): Future[Boolean] = {
    localAsyncTx { implicit s => sql"select pgq.create_queue(${queueName})".map(_.boolean(1)).single.apply().getOrElse(false) }
  }
  override def dropQueue(queueName: String, force: Boolean = false)(implicit ec: ExecutionContext): Future[Boolean] = {
    localAsyncTx { implicit s => sql"select pgq.drop_queue(${queueName}, ${force})".map(_.boolean(1)).single.apply().getOrElse(false) }
  }

  override def registerConsumer(queueName: String, consumerName: String)(implicit ec: ExecutionContext): Future[Boolean] = {
    localAsyncTx { implicit s => sql"select pgq.register_consumer(${queueName}, ${consumerName})".map(_.boolean(1)).single.apply().getOrElse(false) }
  }
  override def unRegisterConsumer(queueName: String, consumerName: String)(implicit ec: ExecutionContext): Future[Boolean] = {
    localAsyncTx { implicit s => sql"select pgq.unregister_consumer(${queueName}, ${consumerName})".map(_.boolean(1)).single.apply().getOrElse(false) }
  }
  
	override def getNextBatchEvents(queueName: String,consumerName: String)(implicit ec: ExecutionContext): Future[Option[(Long, Iterable[Event])]] = {
	  localAsyncTx { implicit s => 
	    nextBatch(queueName, consumerName) map {
	      batchId => batchId -> getBatchEvents(batchId)
	    }
	  }
	}
  override def finishBatch(batchId: Long)(implicit ec: ExecutionContext): Future[Boolean] = {
    localAsyncTx { implicit s => 
	    sql"select pgq.finish_batch(${batchId})".map(_.boolean(1)).single.apply().getOrElse(false)
	  }
  }
  
  private def nextBatch(queueName: String, consumerName: String)(implicit s: DBSession, ec: ExecutionContext): Option[Long] = {
    sql"select next_batch from pgq.next_batch(${queueName}, ${consumerName})"
      .map(rs => rs.longOpt("next_batch"))
      .single()
      .apply()
      .flatten
  }
  private def getBatchEvents(batchId: Long)(implicit s: DBSession, ec: ExecutionContext): Iterable[Event] = {
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
      .apply()
  }
  
  override def getQueueInfo()(implicit ec: ExecutionContext) = {
    localAsyncTx { implicit s => sql"select * from pgq.get_queue_info()"
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
      .apply()
    }
  }
  override def getQueueInfo(queueName: String)(implicit ec: ExecutionContext): Future[Option[QueueInfo]] = {
    localAsyncTx { implicit s => sql"select * from pgq.get_queue_info(${queueName})"
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
      .apply()
    }
  }
  
  override def getConsumerInfo()(implicit ec: ExecutionContext): Future[Seq[ConsumerInfo]] = {
    localAsyncTx { implicit s => sql"select * from pgq.get_consumer_info()"
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
      .apply()
    }
  }
  override def getConsumerInfo(queueName: String)(implicit ec: ExecutionContext): Future[Seq[ConsumerInfo]] = {
    localAsyncTx { implicit s => sql"select * from pgq.get_consumer_info(${queueName})"
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
      .apply()
    }
  }
  override def getConsumerInfo(queueName: String, consumerName: String)(implicit ec: ExecutionContext): Future[Option[ConsumerInfo]] = {
    localAsyncTx { implicit s => sql"select * from pgq.get_consumer_info(${queueName}, ${consumerName})"
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
      .apply()
    }
  }
  
  override def insertEvent(queueName: String, eventType: String, eventData: String, extra1: String = null, extra2: String = null, extra3: String = null, extra4: String = null)(implicit ec: ExecutionContext): Future[Long] = {
    localAsyncTx { implicit s => sql"select pgq.insert_event(${queueName}, ${eventType} , ${eventData}, ${extra1}, ${extra2}, ${extra3}, ${extra4})".map(_.long(1)).single.apply().get }
  }
  
  override def insertEventsTransactionally(queueName: String, eventType: String, eventsData: Seq[String])(implicit ec: ExecutionContext): Future[Seq[Long]] = {
    localAsyncTx { implicit s => 
      eventsData.map { eventData =>
        sql"select pgq.insert_event(${queueName}, ${eventType} , ${eventData})".map(_.long(1)).single.apply().get
      }
    }
  }
  
  implicit val array: TypeBinder[Period] = {
    val mathContext = new java.math.MathContext(4)
    TypeBinder(_ getObject _)(_ getObject _).map{
      case ts: org.postgresql.util.PGInterval => {
        val years = ts.getYears
        val months = ts.getMonths
        val days = ts.getDays
        val hours = ts.getHours
        val mins = ts.getMinutes
        val seconds = Math.floor(ts.getSeconds).asInstanceOf[Int]
        val secondsAsBigDecimal = new java.math.BigDecimal(ts.getSeconds,mathContext)
        val millis = secondsAsBigDecimal.subtract(new java.math.BigDecimal(seconds)).multiply(new java.math.BigDecimal(1000)).intValue
  
        new Period(years,months, 0, days, hours, mins, seconds,millis).normalizedStandard
      }
    }
  }
}