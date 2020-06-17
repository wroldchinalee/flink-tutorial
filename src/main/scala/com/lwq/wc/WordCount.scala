package com.lwq.wc

import org.apache.flink.api.scala._

/**
  * @author: LWQ
  * @create: 2020/6/11
  * @description: WordCount
  **/
object WordCount {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val dataSet: DataSet[String] = env.readTextFile("E:\\source_code\\flink-tutorial\\src\\main\\resources\\wordcount.txt")
    val wordCountDataSet: DataSet[(String, Int)] = dataSet.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)

    wordCountDataSet.print()

  }
}
