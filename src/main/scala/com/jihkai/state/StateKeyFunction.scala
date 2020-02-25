package com.jihkai.state

import org.apache.flink.api.common.functions.{FlatMapFunction, RichFlatMapFunction, RuntimeContext}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor, ListState, ListStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector
import scala.collection.mutable.ListBuffer

import org.apache.flink.api.common.state.StateTtlConfig
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.state.ValueStateDescriptor

object StateKeyFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //TTL
    val statettl = StateTtlConfig
      .newBuilder(Time.seconds(10))
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .build()

    //
    // ValueState[T]
    //
    val input = env.fromElements((3,-2L),(3,4L),(5,2L))
    input.keyBy(_._1).flatMap {
      new RichFlatMapFunction[(Int, Long), (Int, Long, Long)] {
        private var leastValueState: ValueState[Long] = _
        println(leastValueState)
        override def open(parameters: Configuration): Unit ={
          val leastValueStateDescriptor = new ValueStateDescriptor[Long]("leastValue", classOf[Long])
          leastValueState = getRuntimeContext.getState(leastValueStateDescriptor)
        }

        override def flatMap(inputValue: (Int, Long), out: Collector[(Int, Long, Long)]): Unit = {
          val leastValue = leastValueState.value()
          println(leastValue)
          //logic falsehood, init value is 0, if value is positive, the value will always be 0
          if (inputValue._2 > leastValue) {
            //println("check!!!!")
            out.collect((inputValue._1, inputValue._2, leastValue))
          } else {
            leastValueState.update(inputValue._2)
            out.collect((inputValue._1, inputValue._2, inputValue._2))
          }


        }
      }
    }.print

    val inputStream: DataStream[(Int, Long)] = env.fromElements((2, 21L), (2, 1L), (5, 4L), (2, 7L), (2, -10L), (2, 0L))

    val counts: DataStream[(Long, Long)] = inputStream
      .keyBy(_._1)
      .mapWithState((in: (Int, Long), count: Option[Long]) =>
        count match {
          case Some(c) => ((in._1, c), Some(c + in._2))
          case None => ((in._1, 0), Some(in._2))
        })

    counts.print

    env.execute()


  }
}
