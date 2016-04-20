package scalapgq

import akka.NotUsed
import akka.stream._
import akka.stream.stage._
import akka.stream.scaladsl._
import org.joda.time._
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal
import scala.concurrent.Future
import scala.concurrent.duration._

case class Event(
  ev_id: Long,
  ev_time: DateTime,
  ev_txid: Long,
  ev_retry: Option[Int],
  ev_type: Option[String],
  ev_data: Option[String],
  ev_extra1: Option[String],
  ev_extra2: Option[String],
  ev_extra3: Option[String],
  ev_extra4: Option[String]
)

case class QueueInfo(
  queue_name: String,
  queue_ntables: Int,
  queue_cur_table: Int,
  queue_rotation_period: Period,
  queue_switch_time: DateTime,
  queue_external_ticker: Boolean,
  queue_ticker_paused: Boolean,
  queue_ticker_max_count: Int,
  queue_ticker_max_lag: Period,
  queue_ticker_idle_period: Period,
  ticker_lag: Period,
  ev_per_sec: Option[Double],
  ev_new: Long,
  last_tick_id: Long
)

case class ConsumerInfo(
  queue_name: String,
  consumer_name: String,
  lag: Period,
  last_seen: Period,
  last_tick: Long,
  current_batch: Option[Int],
  next_tick: Option[Int],
  pending_events: Long
)

case class BatchInfo(
  queue_name: String,
  consumer_name: String,
  batch_start: DateTime,
  batch_end: DateTime,
  prev_tick_id: Int,
  tick_id: Int,
  lag: Period,
  seq_start: Int,
  seq_end: Int
)

class PGQ(val opsF: PGQSettings => PGQConsumerOperations) {
  def source(settings: PGQSettings): Source[Event, NotUsed] = {
    Source.fromGraph(new PGQSourceGraphStage(settings, opsF))
  }
}

case class PGQSettings(
  val url: String, 
  val user: String, 
  val password: String,
  val queueName: String,
  val consumerName: String,
  val consumerSilencePeriod: FiniteDuration = 500 millis,
  val registerConsumer: Boolean = false
)

class PGQSourceGraphStage(settings: PGQSettings, opsFactory: PGQSettings => PGQConsumerOperations) extends GraphStage[SourceShape[Event]] {
  val out: Outlet[Event] = Outlet("EventsSource")
  override val shape: SourceShape[Event] = SourceShape(out)
  
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) {
      val ops = opsFactory(settings)
      var registrationCompleted = if(settings.registerConsumer) false else true
      
      override def preStart() = {
        if(settings.registerConsumer) {
          implicit val ec = materializer.executionContext
          ops.registerConsumer(settings.queueName, settings.consumerName) onComplete(onRegistrationCallback.invoke)
        }
      }
      
      val onRegistrationCallback = getAsyncCallback[Try[Boolean]] { 
        case Success(_) =>
          registrationCompleted = true
          if(isAvailable(out)) poll()
        case Failure(ex) =>
          failStage(ex)
      }
      
      val onBatchCallback = getAsyncCallback[Try[Option[(Long, Iterable[Event])]]] {
        case Success(Some((batchId,Nil))) => 
          finishBatch(batchId); scheduleOnce(None, settings.consumerSilencePeriod)
        case Success(Some((batchId,events))) =>
          emitMultiple(out, events.iterator, () => finishBatch(batchId))
        case Success(None) =>
          scheduleOnce(None, settings.consumerSilencePeriod)
        case Failure(ex) => 
          failStage(ex)
      }
      
      val onFinishBatchCallback = getAsyncCallback[Try[Boolean]] { 
        case Success(true) => ()
        case Success(false) => failStage(new Exception("finish batch returned false"))
        case Failure(ex) => failStage(ex)
      }
      
      def finishBatch(batchId: Long) = {
        implicit val ec = materializer.executionContext
        ops.finishBatch(batchId) onComplete(onFinishBatchCallback.invoke)
      }
      
      setHandler(out, new OutHandler {
        override def onPull(): Unit = poll()
      })
      
      override def onTimer(timerKey: Any) = {
        poll()
      }
      
      def poll(): Unit = {
        if(registrationCompleted) {
          implicit val ec = materializer.executionContext
          ops.getNextBatchEvents(settings.queueName, settings.consumerName) onComplete(onBatchCallback.invoke)
        }
      }
    }
}

