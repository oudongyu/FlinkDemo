package com.oudongyu.bigdata.Test

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.scala._

object LotteryFlinkAnly {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
//    env.setStateBackend(new RocksDBStateBackend("file:///"))
    val datasLottery = env.readTextFile("datas/lottery.txt")
    datasLottery.map(line=>{
      val strings = line.split(",")
      strings(2)
    })
      .print()
    env.execute()
  }
}
