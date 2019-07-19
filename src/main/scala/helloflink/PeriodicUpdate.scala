package helloflink

import java.util.concurrent.TimeUnit

import helloflink.source.{Tick, TickSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{PurgingTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, Window}
import org.apache.flink.util.Collector

case class MyInt(i: Int)


class MyIntTrigger[W <: Window] extends Trigger[MyInt,W] {

  override def onElement(element: MyInt, timestamp: Long, window: W, ctx: TriggerContext): TriggerResult = {
    //trigger is fired if average marks of a student cross 80
    TriggerResult.FIRE
  }

  override def onProcessingTime(time: Long, window: W, ctx: TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }
  override def onEventTime(time: Long, window: W, ctx: TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(window: W, ctx: TriggerContext) = ???
}

object PeriodicUpdate extends App {

  val port: Int = 9000

  // get the execution environment
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  // get input data by connecting to the socket
  val text = env.socketTextStream("localhost", port, '\n')

  val periodic = env.addSource(new TickSource(1,3,TimeUnit.SECONDS))
    //.keyBy("word")

  val cop = new CoProcessFunction[WordWithCount,Tick,
    WordWithCount] {

    @volatile
    var boost: Long = 0

    override def processElement1(value: WordWithCount, ctx: CoProcessFunction[WordWithCount, Tick, WordWithCount]#Context, out: Collector[WordWithCount]): Unit =
      out.collect(value.copy(count = value.count+boost))

    override def processElement2(value: Tick, ctx: CoProcessFunction[WordWithCount, Tick, WordWithCount]#Context, out: Collector[WordWithCount]): Unit = {
      println(s"boost $boost" )
      boost = boost + 1
    }
  }

  // parse the data, group it, window it, and aggregate the counts
  val windowCounts = text
    .flatMap { w => w.split("\\s") }
    .map { w => MyInt(w.toInt) }
    .setParallelism(1)
    //.connect(periodic).process(cop)
    .join(periodic).where(_.i).equalTo(t=>t.value).window(GlobalWindows.create())
    .trigger(PurgingTrigger.of(new MyIntTrigger[GlobalWindow]()))
    .apply((a,b) => a )
    .setParallelism(1)
//    .timeWindow(Time.seconds(5), Time.seconds(1))





  windowCounts
  .print().setParallelism(1)

  // print the results with a single thread, rather than in parallel

  env.execute("Socket Window WordCount")

}

case class WordWithCount(word: String, count: Long)