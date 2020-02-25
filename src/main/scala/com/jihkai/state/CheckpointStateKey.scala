package com.jihkai.state

import java.util.Properties

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import org.slf4j.LoggerFactory

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend

import com.jihkai.config.KafkaConfig

object CheckpointStateKey {
  private val LOG = LoggerFactory.getLogger(CheckpointStateKey.getClass)
  private val KAFKA_CONSUMER_TOPIC="test"
  private val KAFKA_PROP: Properties = new Properties() {
    setProperty("bootstrap.servers", KafkaConfig.KAFKA_BROKERS)
    setProperty("zookeeper.connect", KafkaConfig.KAFKA_ZOOKEEPER_CONNECTION)
    setProperty("group.id", KafkaConfig.KAFKA_GROUP_ID)
    setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //setProperty("auto.offset.reset", "earliest")
  }



  def main(args: Array[String]): Unit = {
    LOG.info("===Stateful Computation Demo===")
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.enableCheckpointing(5000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setRestartStrategy(RestartStrategies.noRestart())
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    //env.setStateBackend(new RocksDBStateBackend("file:///Users/jihkai.lin/Downloads/FlinkCheckpoints/9e54a9fda47ea0689dd38b7056028621/chk-17/_metadata"));

    val checkPointPath = new Path("file:///Users/jihkai.lin/Downloads/FlinkCheckpoints")
    val fsStateBackend: StateBackend= new FsStateBackend(checkPointPath)
    env.setStateBackend(fsStateBackend)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    val stream = env
      .addSource(new FlinkKafkaConsumer[String](KAFKA_CONSUMER_TOPIC,new SimpleStringSchema(),KAFKA_PROP))

    stream.filter(_.split("\\|").length==3)
      .map(line=>{
        val arr = line.split("\\|")
        (arr(0),arr(2).toInt)
      })
      .keyBy(_._1)
      .flatMap(new SalesAmountCalculation())
      .print

    env.execute()

  }
}

class SalesAmountCalculation extends RichFlatMapFunction[(String, Int), (String, Int)] {
  private var sum: ValueState[(String, Int)] = _
  override def flatMap(input: (String, Int), out: Collector[(String, Int)]): Unit = {

    val tmpCurrentSum = sum.value
    val currentSum = if (tmpCurrentSum != null) {
      tmpCurrentSum
    } else {
      (input._1, 0)
    }
    val newSum = (currentSum._1, currentSum._2 + input._2)
    sum.update(newSum)
    out.collect(newSum)
  }

  override def open(parameters: Configuration): Unit = {

    val valueStateDescriptor = new ValueStateDescriptor[(String, Int)]("sum", createTypeInformation[(String, Int)])

    sum = getRuntimeContext.getState(valueStateDescriptor)
  }
}

