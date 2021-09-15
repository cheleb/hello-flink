package helloflink

import java.util.concurrent.TimeUnit

import helloflink.source.{Tick, TickSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{
  GlobalWindows,
  TumblingEventTimeWindows
}
import org.apache.flink.streaming.api.windowing.triggers.{
  PurgingTrigger,
  Trigger,
  TriggerResult
}
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, Window}
import org.apache.flink.util.Collector
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{
  CountTrigger,
  PurgingTrigger,
  Trigger,
  TriggerResult
}
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, Window}
import _root_.function.DedupFunction
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.configuration.Configuration
import java.util.UUID
import java.io.File

import org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import _root_.function._

class MyIntTrigger2[W <: Window] extends Trigger[AnyRef, W] {

  override def onElement(
      element: AnyRef,
      timestamp: Long,
      window: W,
      ctx: TriggerContext
  ): TriggerResult = {
    //trigger is fired if average marks of a student cross 80
    TriggerResult.FIRE_AND_PURGE
  }

  override def onProcessingTime(
      time: Long,
      window: W,
      ctx: TriggerContext
  ): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def onEventTime(
      time: Long,
      window: W,
      ctx: TriggerContext
  ): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(window: W, ctx: TriggerContext) = ()
}

object PeriodicUpdate2 extends App {

  val conf = new Configuration()
  conf.setInteger("rest.port", 9000)
  val fsStateBackendPath =
    "file:///tmp/flink-state"

  // get the execution environment
  val env: StreamExecutionEnvironment =
    StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
  env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE)
  env.setStateBackend(new FsStateBackend(fsStateBackendPath, false))

  // get input data by connecting to the socket
  //val text = env.socketTextStream("localhost", port, '\n')

  val src = new TickSource(1, 10, TimeUnit.MILLISECONDS)

  val periodic = env.addSource(src).setParallelism(10)

  periodic
    .flatMap(t => List(t, t.copy(action = "impression")))
    .keyBy(v => v.uuid.getOrElse(UUID.randomUUID()).toString + v.action)
    //.keyBy( v => v.uuid.getOrElse(UUID.randomUUID()).toString )
    .process(new DedupFunctionValueStateAsMap(60))
    .setParallelism(4)

  // print the results with a single thread, rather than in parallel

  env.execute("Socket Window WordCount")

}
