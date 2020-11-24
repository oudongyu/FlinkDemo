package com.oudongyu.bigdata

import java.{lang, util}
import java.util.Properties

import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import scala.actors.threadpool.TimeUnit

case class SensorReading(id: String, timestamp: Long, temperature: Double)

class coMap extends CoMapFunction[(String, Int), Int, Int] {
    override def map1(in1: (String, Int)): Int = in1._2
//  override def map1(in1: Int): Int = in1

  override def map2(in2: Int): Int = in2
}

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

    /*env.addSource(new MySensorSource).addSink(new MyjdbcSink)
    env.execute()*/
    /*val path = "datas/a.txt"
    val value = env.readFile[String](new TextInputFormat(new Path(path)),
      path,
      FileProcessingMode.PROCESS_ONCE,
      1)
    value.print()
    env.fromCollection(Array(1,2,3)).print()
    env.generateSequence(1,100).print()*/
    //    env.fromCollection(CustomIterator,BasicTypeInfo.INT_TYPE_INFO).print()
    val sink = StreamingFileSink.forRowFormat(new Path("./datas/output"), new SimpleStringEncoder[String]("UTF-8"))
      .withRollingPolicy(DefaultRollingPolicy.create()
        .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
        .withMaxPartSize(1024 * 1024 * 1024)
        .build()).build()

    /*val value: DataStream[Long] = env.addSource(new RichSourceFunction[Long] {
      var isRunning = true
      var count = 0L

      override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
        while (isRunning && count < 1000) {
          ctx.collect(count)
          count += 1
        }
      }

      override def cancel(): Unit = isRunning = false
    })*/
    //    value
    //      .addSink(new BucketingSink[Long]("./datas/output01"))
    //      .writeAsText("./datas/output01",FileSystem.WriteMode.OVERWRITE)
    //      .print()
    /*val properties = new Properties()
    properties.setProperty("bootstrap.servers", "houda:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    env.addSource(new FlinkKafkaConsumer[String]("flink-stream-in-topic", new SimpleStringSchema(), properties))
      .map(line=>
        {
          val strings = line.split(" ")
          (strings(0),strings(1).toInt)
        }).keyBy(0).sum(1)
        //        .keyBy(0).sum(1)
      .print()*/
    /*val streamSource = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
    val split = streamSource.split(new OutputSelector[Int] {
      override def select(out: Int): lang.Iterable[String] = {
        val strings = new util.ArrayList[String]()
        strings.add(if (out % 2 == 0) "even" else "odd")
        strings
      }
    })
    split.select("odd").print()*/

    val value1: DataStream[(String, Int)] = env.fromElements(("a", 1), ("a", 2))
    //    val value2 = env.fromElements(("b", 1), ("b", 2))
    val value2: DataStream[Int] = env.fromElements(1, 2)
    //    val value3 = value1.union(value2)
    //    value3.print()

    val mapResult: ConnectedStreams[(String, Int), Int] = value1.connect(value2)
    mapResult.map(new coMap).print()

    //    value1.shuffle.print()
    /*value1.rebalance.print()
    value1.rescale.print()
    val value: BroadcastStream[(String, Int)] = value1.broadcast()
    println(value)*/
    env.execute()

  }
}
