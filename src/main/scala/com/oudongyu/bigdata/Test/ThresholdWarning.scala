package com.oudongyu.bigdata.Test

import java.util

import org.apache.commons.compress.utils.Lists
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, StateTtlConfig}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import java.util.ArrayList

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala._

class ThresholdWarning extends RichFlatMapFunction[(String, Long), (String, java.util.ArrayList[Long])] with CheckpointedFunction{
  var abnormalData: ListState[Long] = _
  var threshold: Long = _


  def this(threshold: Long) {
    this()
    this.threshold = threshold
  }

  override def open(parameters: Configuration): Unit = {
    abnormalData = getRuntimeContext.getListState(new ListStateDescriptor("abnormalData", classOf[Long]))
  }

  override def flatMap(in: (String, Long), collector: Collector[(String, java.util.ArrayList[Long])]): Unit = {
    val inputValue: Long = in._2
    if (inputValue >= threshold) {
      abnormalData.add(inputValue)
    }
    val longs: java.util.ArrayList[Long] = Lists.newArrayList(abnormalData.get().iterator())
    if (longs.size() >= 3) {
      collector.collect((in._1 + "超过阈值", longs))
      abnormalData.clear()
    }
  }

  override def snapshotState(functionSnapshotContext: FunctionSnapshotContext): Unit = ???

  override def initializeState(functionInitializationContext: FunctionInitializationContext): Unit = ???
}

object ThresholdWarning {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(new RocksDBStateBackend("file:///D:\\IdeaProjects\\FlinkDemo\\datas\\aabbcc"))
    env.setParallelism(4)
    val datas = env.fromElements(("a", 100L), ("a", 10L), ("b", 1L), ("a", 101L), ("a", 102L))
    datas.keyBy(0)
      .flatMap(new ThresholdWarning(100L)).printToErr()
    env.execute()
  }
}
