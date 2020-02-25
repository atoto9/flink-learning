package com.jihkai.state

import java.util.Properties

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext, StateBackend}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

import com.jihkai.config.KafkaConfig

object CheckpointStateOperate {
  private val LOG = LoggerFactory.getLogger(CheckpointStateOperate.getClass)
  private val KAFKA_CONSUMER_TOPIC = "test"
  private val KAFKA_PROP: Properties = new Properties() {
    setProperty("bootstrap.servers", KafkaConfig.KAFKA_BROKERS)
    setProperty("zookeeper.connect", KafkaConfig.KAFKA_ZOOKEEPER_CONNECTION)
    setProperty("group.id", KafkaConfig.KAFKA_GROUP_ID)
    setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    setProperty("auto.offset.reset", "earliest")
  }

  def main(args: Array[String]): Unit = {
    LOG.info("===Stateful Computation Demo===")
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.enableCheckpointing(5000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setRestartStrategy(RestartStrategies.noRestart())
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

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
      .addSink(new BufferingSink(3))

    env.execute()

  }
}

class BufferingSink(threshold: Int= 0) extends SinkFunction[(String, Int)] with CheckpointedFunction {

  private val LOG = LoggerFactory.getLogger("BufferingSink")

  @transient
  private var checkPointedState: ListState[(String, Int)] = _
  private val bufferedElements = ListBuffer[(String, Int)]()


  override def invoke(value: (String, Int)):Unit = {
    bufferedElements += value
    if (bufferedElements.size == threshold) {
      for (element <- bufferedElements) {
        LOG.error(s"==================BufferingSink invoke,elements approach the threshold,Print==================$element")
      }
      bufferedElements.clear()
    }
  }


  override def snapshotState(context: FunctionSnapshotContext) = {
    checkPointedState.clear()
    for (element <- bufferedElements) {
      checkPointedState.add(element)
    }
  }



  override def initializeState(context: FunctionInitializationContext) = {
    val descriptor = new ListStateDescriptor[(String, Int)]("buffered-elements", TypeInformation.of(new TypeHint[(String, Int)]() {}))
    checkPointedState = context.getOperatorStateStore.getListState(descriptor)
    LOG.info(s"********************context state********************" + context.isRestored)

    if (context.isRestored) {
      LOG.info("********************has element state?********************" + checkPointedState.get().iterator().hasNext)
      val it = checkPointedState.get().iterator()
      while (it.hasNext) {
        val element = it.next()
        LOG.error(s"********************checkpoint restore,the prefix state value********************($element)")
        bufferedElements += element

      }
    }
  }
}