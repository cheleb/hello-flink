package helloflink

import java.util.concurrent.TimeUnit

import helloflink.source.{Tick, TickSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.triggers.{PurgingTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, Window}
import org.apache.flink.util.Collector
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, PurgingTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, Window}


case class MyInt(i: Int)


class MyIntTrigger[W <: Window] extends Trigger[AnyRef, W] {

  override def onElement(element: AnyRef, timestamp: Long, window: W, ctx: TriggerContext): TriggerResult = {
    //trigger is fired if average marks of a student cross 80
    TriggerResult.FIRE_AND_PURGE
  }

  override def onProcessingTime(time: Long, window: W, ctx: TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def onEventTime(time: Long, window: W, ctx: TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(window: W, ctx: TriggerContext) = ()
}

object PeriodicUpdate extends App {

  val port: Int = 9000

  // get the execution environment
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  // get input data by connecting to the socket
  val text = env.socketTextStream("localhost", port, '\n')

  val periodic = env.addSource(new TickSource(1, 1, TimeUnit.MILLISECONDS))

  val windowCounts = env.addSource(new TickSource(3, 1, TimeUnit.MILLISECONDS))

  // parse the data, group it, window it, and aggregate the counts
//  val windowCounts = text
//    .flatMap { w => w.split("\\s") }
//    .map { w =>
//      MyInt(w.toInt)
//    }

    .setParallelism(1)
    .join(periodic).where(_.value).equalTo(t => t.value)
    .window(GlobalWindows.create())
    .trigger(PurgingTrigger.of(CountTrigger.of[GlobalWindow](2)))
    .apply {
    (a, b) =>
       a.copy(value=a.value + b.value)
  }


  //    .timeWindow(Time.seconds(5), Time.seconds(1))


  windowCounts
    .print().setParallelism(1)

  // print the results with a single thread, rather than in parallel

  env.execute("Socket Window WordCount")

}

case class WordWithCount(word: String, count: Long)