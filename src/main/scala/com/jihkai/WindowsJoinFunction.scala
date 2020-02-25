package com.jihkai

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object WindowsJoinFunction {
  case class Age(name:String, age:Int)
  case class Sex(name:String, sex:String)
  case class Person(name:String, sex:String, age:Int)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val input1 = env.fromCollection(List(Age("Daniel", 22), Age("John", 31), Age("Peggy", 44)))
    val input2 = env.fromCollection(List(Sex("Daniel", "1"), Sex("John", "1"), Sex("Hans", "1")))

    val joinWindows = joinStreams(input1, input2, 3)
    //val jointest = input1.join(input2)
    //  .where(_.name)
    //  .equalTo(_.name)
    //  .window(TumblingEventTimeWindows.of(Time.seconds(3))).apply((x,y) => Person(x.name, y.sex, x.age))


    //input1.print
    //jointest.print

    joinWindows.print

    env.execute()

  }



  def joinStreams(
                   age: DataStream[Age],
                   sex: DataStream[Sex],
                   windowSize: Long) : DataStream[Person] = {

    age.join(sex)
      .where(_.name)
      .equalTo(_.name)
      .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
      .apply { (a, s) => Person(a.name, s.sex, a.age) }
  }
}
/*
class WindowFunctionTest extends WindowFunction[(String,Long),(String, Int,String,String,String,String),String,TimeWindow]{

  override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[(String, Int,String,String,String,String)]): Unit = {
    val list = input.toList.sortBy(_._2)
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    out.collect(key,input.size,format.format(list.head._2),format.format(list.last._2),format.format(window.getStart),format.format(window.getEnd))
  }

}


 */