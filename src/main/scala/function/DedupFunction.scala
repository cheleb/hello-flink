package function

import org.apache.flink.api.common.state.{ StateTtlConfig, ValueState, ValueStateDescriptor }
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


class DedupFunction(ttl: Int)
    extends KeyedProcessFunction[String, Tick, Tick] 
     {

  override def close(): Unit = 
    println("DISP")


  @transient
  private var operatorState: MapState[String, Int] = _

  var evictedCount: Counter = _

  override def open(configuration: Configuration): Unit = {
    super.open(configuration)

    evictedCount = getRuntimeContext.getMetricGroup.counter("duplicated-evicted")

    

val ttlConfig: StateTtlConfig = StateTtlConfig
      .newBuilder(Time.seconds(ttl))
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
//      .cleanupIncrementally(1000, true)
      .build

    val descriptor: MapStateDescriptor[String, Int] =
      new MapStateDescriptor[String, Int](
        "seen",
        createTypeInformation[String],
        createTypeInformation[Int],
      )
    descriptor.enableTimeToLive(ttlConfig)

    operatorState = getRuntimeContext.getMapState(descriptor)

  }

  override def processElement(
    value: Tick,
    ctx:   KeyedProcessFunction[String, Tick, Tick]#Context,
    out:   Collector[Tick],
  ): Unit ={

  val action = value.uuid.get.toString()
   if (operatorState.contains(action)) {
      val count = operatorState.get(action)
      if (count < 1) {
        operatorState.put(action, count + 1)
        out.collect(value)
      } else {
        out.collect(
          value

        )
      }
    } else {
      out.collect(value)
      operatorState.put(action, 1)
    }
  }

}
