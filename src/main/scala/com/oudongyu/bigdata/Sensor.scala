package com.oudongyu.bigdata

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import com.oudongyu.bigdata.MySensorSource

case class SensorReading(id: String, timestamp: Long, temperature: Double)

object Sensor {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    /*val stream1 = env.fromCollection(List(
      SensorReading("sensor_1", 15664731, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1)))
    stream1.print("stream1:").setParallelism(2)
    env.execute("streamSensor")*/

    /*val fileData = env.readTextFile("datas/a.txt")
    fileData.print("stream2:").setParallelism(1)
    env.execute()*/

    env.addSource(new MySensorSource).addSink(new MyjdbcSink)
    env.execute()
  }
}
