package helloflink

import org.apache.flink.api.common.state.CheckpointListener
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext, StateInitializationContext, StateInitializationContextImpl, StateSnapshotContextSynchronousImpl}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

class KafkaGracefull extends ProcessFunction[String, String] with CheckpointListener with CheckpointedFunction {
  override def processElement(value: String, ctx: ProcessFunction[String, String]#Context, out: Collector[String]): Unit = {

  }

  override def notifyCheckpointComplete(checkpointId: Long): Unit = {

  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
     println(context.toString)
    context match {
      case context: StateSnapshotContextSynchronousImpl =>
        println(context.toString)
    }
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    println(s"Init ${context.toString} " )
    context match {
      case context: StateInitializationContextImpl =>
        println(context.isRestored)
      case _ =>
    }

  }
}
