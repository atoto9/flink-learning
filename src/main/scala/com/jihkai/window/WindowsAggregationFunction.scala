package com.jihkai.window

import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object WindowsAggregationFunction {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source = env.socketTextStream("localhost",9000)

    val r = scala.util.Random

    val values:DataStream[(String, Int)] = source.flatMap(value => value.split("\\s+")).map(value => (value, r.nextInt(10)))




    //val keyValue = values.keyBy(0)

    //tumbling window : Calculate wordcount for each 15 seconds

    //val tumblingWindow = keyValue.timeWindow(Time.seconds(3))
    // sliding window : Calculate wordcount for last 5 seconds
    //val slidingWindow = keyValue.timeWindow(Time.seconds(15),Time.seconds(5))
    //count window : Calculate for every 5 records
    //val countWindow = keyValue.countWindow(5)


    //tumblingWindow.sum(1).name("tumblingwindow").print()
    //slidingWindow.sum(1).name("slidingwindow").print()
    //countWindow.sum(1).name("count window").print()

    ///
    /// reducefunction
    /// input type as the same as output time
    ///

    val reduceWindowStream = values
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .reduce(new ReduceFunction[(String, Int)] {
      override def reduce(t1: (String, Int), t2: (String, Int)): (String, Int) = {
        (t1._1, t1._2.toInt + t2._2.toInt)
      }
    })

    //values.print
    //reduceWindowStream.print

    ///
    /// aggregatefunction
    ///

    val aggregateWindowStream = values
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .aggregate(new MyAverageAggregate)

    //values.print
    //aggregateWindowStream.print

    ///
    /// processwindowfunction
    ///
    val staticsStream = values
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .process(new StaticsProcessFunction)

    //values.print
    //staticsStream.print

    val result = values
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .reduce(
        (r1: (String, Int), r2: (String, Int)) => {
          if (r1._2 > r2._2) r2 else r1
        }
        ,
        (key: String,
         window: TimeWindow,
         minReadings: Iterable[(String, Int)],
         out: Collector[(Long, (String, Int))]) => {
          val min = minReadings.iterator.next()
          out.collect((window.getEnd, min))
        })

    values.print
    result.print

    env.execute()

  }


}

class MyAverageAggregate extends AggregateFunction[(String, Int), (Long, Long), Double] {
  override def createAccumulator() = (0L, 0L)
  override def add(value: (String, Int), accumulator: (Long, Long)) =
    (accumulator._1 + value._2.toLong, accumulator._2 + 1L)
  override def getResult(accumulator: (Long, Long)) = accumulator._1.toDouble / accumulator._2.toDouble
  override def merge(a: (Long, Long), b: (Long, Long)) =
    (a._1 + b._1, a._2 + b._2)
}

class StaticsProcessFunction extends ProcessWindowFunction[(String, Int), (String, Long, Long, Long, Long, Long), String, TimeWindow]{
  override def process(
                      key: String,
                      ctx: Context,
                      vals: Iterable[(String, Int)],
                      out:Collector[(String, Long, Long, Long, Long, Long)]): Unit = {
    val sum = vals.map(_._2).sum.toLong
    val min = vals.map(_._2).min.toLong
    val max = vals.map(_._2).max.toLong
    val avg = sum / vals.size
    val windowEnd = ctx.window.getEnd
    out.collect((key, sum, min, max, avg, windowEnd))
  }
}

class ParseDate extends RichMapFunction[Long, Long] {
  var parallelism = 0L
  var idCounter = 0L

  override def open(parameters: Configuration): Unit = {
    val ctx = getRuntimeContext
    parallelism = ctx.getNumberOfParallelSubtasks
    idCounter = ctx.getIndexOfThisSubtask
  }

  def map(value: Long): Long = {
    idCounter += parallelism
    // further processing
    idCounter
  }
}
