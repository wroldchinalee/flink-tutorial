package com.lwq.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
  * @author: LWQ
  * @create: 2020/6/11
  * @description: StreamWordCount
  **/
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val params = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")

    val textDataStream: DataStream[String] = env.socketTextStream(host, port)

    val wordCountDataStream = textDataStream.filter(_.nonEmpty).flatMap(_.split("\\s"))
      .map((_, 1)).keyBy(0).sum(1)

    wordCountDataStream.print()

    env.execute("stream word count")

  }

}
