package com.oudongyu.bigdata

import java.util.Random

import org.apache.flink.streaming.api.functions.source.SourceFunction

class MySensorSource extends SourceFunction[SensorReading] {
  var running: Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    val random = new Random()
    var curTemp = 1.to(10).map(i => ("sensor_" + i, random.nextGaussian() * 20))
    while (running) {
      curTemp = curTemp.map(t => (t._1, t._2 + random.nextGaussian()))
      val curTime = System.currentTimeMillis()
      curTemp.foreach(t => sourceContext.collect(SensorReading(t._1, curTime, t._2)))
      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = running = false
}
