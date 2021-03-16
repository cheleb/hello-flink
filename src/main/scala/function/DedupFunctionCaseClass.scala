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





class DedupFunctionCaseClass(ttl: Int)
    extends KeyedProcessFunction[String, Tick, Tick] 
     {

  override def close(): Unit = 
    println("DISP")


  @transient
  private var operatorState: ValueState[DedupCaseClasse] = _

  var evictedCount: Counter = _

  override def open(configuration: Configuration): Unit = {
    super.open(configuration)

    evictedCount = getRuntimeContext.getMetricGroup.counter("duplicated-evicted")

    
case class DedupCaseClasse(click: Int, impression: Int, other: Int)
val ttlConfig: StateTtlConfig = StateTtlConfig
      .newBuilder(Time.seconds(ttl))
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .build

    val descriptor: ValueStateDescriptor[DedupCaseClasse] =
      new ValueStateDescriptor[DedupCaseClasse](
        "seen", classOf[DedupCaseClasse]
      )
    descriptor.enableTimeToLive(ttlConfig)

    operatorState = getRuntimeContext.getState(descriptor)

  }

  override def processElement(
    value: Tick,
    ctx:   KeyedProcessFunction[String, Tick, Tick]#Context,
    out:   Collector[Tick],
  ): Unit ={

  val action = value.uuid.get.toString()
  val cache = Option(operatorState.value()).getOrElse(DedupCaseClasse(0,0,0))
  val (updated, ok) =  action match {
    case "click" => (cache.copy(click = cache.click+1), cache.click < 1)
    case "impression" => (cache.copy(impression = cache.impression+1), cache.impression < 1)
    case _ => (cache.copy(other = cache.other+1), cache.other < 1)
  }

  operatorState.update(updated)
      if (ok) {
        out.collect(value)
      }

    }
}
