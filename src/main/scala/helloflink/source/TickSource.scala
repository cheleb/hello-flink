package helloflink.source

import java.time.LocalDateTime
import java.util.concurrent.{ExecutorService, Executors, ScheduledExecutorService, ScheduledFuture, TimeUnit}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

case class Tick(word: String, localDateTime: LocalDateTime)

class TickSource(initialDelay: Long, delay: Long, timeUnit: TimeUnit) extends RichSourceFunction[Tick]{


  private val waitDelay = TimeUnit.MILLISECONDS.convert(delay, timeUnit)

  @volatile
  private var isRunning = true


  override def open(parameters: Configuration): Unit = {
  }


  override def run(ctx: SourceFunction.SourceContext[Tick]): Unit = {


    while (isRunning) {
      ctx.collect(Tick("kkk", LocalDateTime.now()))
      synchronized(
      wait( waitDelay ))
    }



  }

  override def cancel(): Unit = {
    isRunning = false;
    printf("stopping")
  }
}
