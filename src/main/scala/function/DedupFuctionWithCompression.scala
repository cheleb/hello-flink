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

class DedupFunctionWithCompression(ttl: Int)
    extends KeyedProcessFunction[String, Tick, Tick] {

  override def close(): Unit =
    println("DISP")

  @transient
  private var operatorState: ValueState[Map[Either[String, Int], Int]] = _

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

    val descriptor: ValueStateDescriptor[Map[Either[String, Int], Int]] =
      new ValueStateDescriptor[Map[Either[String, Int], Int]](
        "seen",
        classOf[Map[Either[String, Int], Int]]
      )
    descriptor.enableTimeToLive(ttlConfig)
    operatorState = getRuntimeContext.getState(descriptor)

  }

  def conv(action: String): Either[String, Int] = action match {
    case "click"      => Right(0)
    case "impression" => Right(1)
    case other        => Left(other)
  }

  override def processElement(
      value: Tick,
      ctx: KeyedProcessFunction[String, Tick, Tick]#Context,
      out: Collector[Tick]
  ): Unit = {

    val action = conv(value.action)

    val cache = Option(operatorState.value())
      .getOrElse(Map.empty[Either[String, Int], Int])

    val count = cache.getOrElse(action, 0);

    operatorState.update(cache + (action -> (count + 1)))
    if (count < 1) {
      out.collect(value)
    }

  }
}
