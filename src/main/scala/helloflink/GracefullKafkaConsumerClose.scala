package helloflink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo
import org.apache.flink.runtime.io.network.api.{CancelCheckpointMarker, CheckpointBarrier}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.runtime.io.CheckpointBarrierHandler

import java.util.Properties

object GracefullKafkaConsumerClose extends App {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  env.enableCheckpointing(1000)


   val properties = new Properties()
  properties.setProperty("bootstrap.servers", "localhost:9092")
  properties.setProperty("group.id", "test")
  val stream = env
    .addSource(new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), properties))

  stream.process(new KafkaGracefull)

  stream
    .print().setParallelism(1)

  // print the results with a single thread, rather than in parallel

  env.execute("Socket Window WordCount")

  println(env.getMaxParallelism)


}
