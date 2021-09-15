package function

import org.apache.flink.api.common.state.{
  StateTtlConfig,
  ValueState,
  ValueStateDescriptor
}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.apache.flink.util.Disposable
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.state.MapState
import helloflink.source.Tick
import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.common.state.ListStateDescriptor

import scala.collection.JavaConverters._

class DedupFunctionWithCompression3(ttl: Int)
    extends KeyedProcessFunction[String, Tick, Tick] {

  override def close(): Unit =
    println("DISP")

  @transient
  private var operatorState: ListState[Int] = _

  var evictedCount: Counter = _

  override def open(configuration: Configuration): Unit = {
    super.open(configuration)

    evictedCount =
      getRuntimeContext.getMetricGroup.counter("duplicated-evicted")

    val ttlConfig: StateTtlConfig = StateTtlConfig
      .newBuilder(Time.seconds(ttl))
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .build

    val descriptor: ListStateDescriptor[Int] =
      new ListStateDescriptor[Int](
        "seen",
        classOf[Int]
      )
    descriptor.enableTimeToLive(ttlConfig)
    operatorState = getRuntimeContext.getListState(descriptor)

  }

  def conv(action: String): Int = action match {
    case "click"      => 0
    case "impression" => 1
  }

  override def processElement(
      value: Tick,
      ctx: KeyedProcessFunction[String, Tick, Tick]#Context,
      out: Collector[Tick]
  ): Unit = {

    val action = conv(value.action)

    val count = operatorState.get().asScala.count(_ == action)

    if (count < 1) {
      operatorState.add(action)
      out.collect(value)
    }

  }
}
