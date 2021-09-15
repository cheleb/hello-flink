package helloflink.source

import java.time.LocalDateTime
import java.util.concurrent.{ExecutorService, Executors, ScheduledExecutorService, ScheduledFuture, TimeUnit}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction
import java.util.UUID

case class Tick(uuid: Option[UUID],value: Int, localDateTime: LocalDateTime, action: String="click")

class TickSource(initialDelay: Long, delay: Long, timeUnit: TimeUnit) extends ParallelSourceFunction[Tick]{


  private val waitDelay = timeUnit.convert(delay, TimeUnit.MILLISECONDS)

  @volatile
  private var isRunning = true

  @volatile
  private var count = 0



  override def run(ctx: SourceFunction.SourceContext[Tick]): Unit = {


    while (isRunning) {
      count = count + 1
      ctx.collect(Tick(if(count % 10==11) None else Some(UUID.randomUUID()), count, LocalDateTime.now()))
      synchronized(
        wait( waitDelay ))
        if(false && count>10) cancel()
    }



  }

  override def cancel(): Unit = {
    isRunning = false;
    printf("stopping")
  }
}
