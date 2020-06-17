package com.lwq.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

import scala.util.Random

/**
  * @author: LWQ
  * @create: 2020/6/15
  * @description: SimpleSourceTest
  **/

case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SimpleSourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 1.从集合中读取
    val dataStream1 = env.fromCollection(List(SensorReading("sensor_1", 1592185873, 34.5),
      SensorReading("sensor_2", 1592185373, 34.3),
      SensorReading("sensor_4", 1592185373, 34.2),
      SensorReading("sensor_5", 1592185373, 34.6)
    ))

    // 2.从文件中读取
    val dataStream2 = env.readTextFile("E:\\source_code\\flink-tutorial\\src\\main\\resources\\sensor.txt")

    // 3.从kafka中读取
    val properties = new Properties()
    properties.put("bootstrap.servers", "bigdata01:9092")
    properties.put("group.id", "consumer-group")
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("auto.offset.reset", "latest")

    val dataStream3 = env.addSource(new FlinkKafkaConsumer010[String]("test1", new SimpleStringSchema(), properties))

    val dataStream4 = env.addSource(new MySource())
    //    dataStream1.print("collection sensor print")
    //    dataStream2.print("textfile sensor print")
    //    dataStream3.print("kafka sensor pint")
    dataStream4.print("mysource sensor print")

    env.execute()
  }
}

class MySource extends RichSourceFunction[SensorReading] {
  var flag: Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random()
    var curTmp = (1 to 10).map(i => {
      SensorReading("sensor_" + i, 124124125 + rand.nextInt(2329), 38.34 + rand.nextGaussian())
    })
    while (flag) {
      curTmp.foreach(
        sourceContext.collect(_)
      )
    }
  }

  override def cancel(): Unit = {
    flag = false
  }
}