package com.oudongyu.bigdata.Test

import org.apache.flink.api.common.serialization.{SerializationSchema, SimpleStringSchema}
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._

object ChainDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironment(1)
    //    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val datas: DataStream[(Int, Int)] = env.fromElements(1, 2, 3, 1, 2, 3, 11, 22, 33, 11, 1, 11, 22, 22)
      .map((_, 1)).disableChaining().map(x => (x._2, x._1))
      .keyBy(0)
      .sum(1)
//          .slotSharingGroup("a")
    val datas02: DataStream[String] = env.fromElements("a", "s", "q")
//          .slotSharingGroup("b")
    val value: ConnectedStreams[(Int, Int), String] = datas.connect(datas02)
    value.map(new MyCoMap)
//      .writeAsCsv("./datas/csv")
//      .print()
//      datas.writeToSocket("houda",4545,new SimpleStringSchema())
    //      .print()
    //    datas.startNewChain()
    //    val plan: String = env.getExecutionPlan
    //    println(plan)
    env.execute()

    class MyCoMap extends CoMapFunction[(Int, Int), String, (String, String)] {


      override def map2(in2: String): (String, String) = (in2, in2)

      override def map1(in1: (Int, Int)): (String, String) = (in1._1.toString,in1._2.toString)
    }
  }
}
