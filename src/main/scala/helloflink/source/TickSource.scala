package helloflink.source

import java.time.LocalDateTime
import java.util.concurrent.{ExecutorService, Executors, ScheduledExecutorService, ScheduledFuture, TimeUnit}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

case class Tick(value: Int, localDateTime: LocalDateTime)

class TickSource(initialDelay: Long, delay: Long, timeUnit: TimeUnit) extends RichSourceFunction[Tick]{


  private val waitDelay = timeUnit.convert(delay, TimeUnit.MILLISECONDS)

  @volatile
  private var isRunning = true

  @volatile
  private var count = 0

  override def open(parameters: Configuration): Unit = {
  }


  override def run(ctx: SourceFunction.SourceContext[Tick]): Unit = {


    while (isRunning) {
      count = (count + 1) % 10
      ctx.collect(Tick(count, LocalDateTime.now()))
      synchronized(
      wait( waitDelay ))
    }



  }

  override def cancel(): Unit = {
    isRunning = false;
    printf("stopping")
  }
}
