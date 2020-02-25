package com.jihkai

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor
import org.apache.flink.streaming.api.windowing.time.Time

object WindowsTriggerFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source = env.socketTextStream("localhost",9000)

    val r = scala.util.Random

    val values:DataStream[(String, Int)] = source.flatMap(value => value.split("\\s+")).map(value => (value, r.nextInt(50)))

    val windowTriggerStream = values
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(5)))
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(t1: (String, Int), t2: (String, Int)): (String, Int) = {
          (t1._1, t1._2.toInt + t2._2.toInt)
        }
      })

    //values.print
    //windowTriggerStream.print

    val windowEvictorStream = values
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .evictor(CountEvictor.of(3))
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(t1: (String, Int), t2: (String, Int)): (String, Int) = {
          (t1._1, t1._2.toInt * t2._2.toInt)
        }
      })

    values.print
    windowEvictorStream.print

    env.execute()

  }
}


