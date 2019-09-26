package com.atguigu.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * @Author cuihaiyan
  * @Create_Time 2019-09-24 14:46
  */
object WorkCount {

  //程序入口
  def main(args: Array[String]): Unit = {

    //设定spark计算框架的运行环境
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WorkCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)
    //println(sc)

    //读取文件：一行一行读取
    val lines: RDD[String] = sc.textFile("in")

    //拆解：将内容按照空格拆解
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //转换：将单词数据进行结构的转换
    val wordToOne: RDD[(String, Int)] = words.map((_, 1))

    //聚合：将转换结构之后的数据进行分组聚合
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)

    //tuple
    val result: Array[(String, Int)] = wordToSum.collect()

    //println(wordToSum.collect())
    //输出到打印台
    result.foreach(println)
  }
}
