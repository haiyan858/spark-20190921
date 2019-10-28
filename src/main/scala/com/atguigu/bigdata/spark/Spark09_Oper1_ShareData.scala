package com.atguigu.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 三大数据类型：
  * RDD
  * 累加器
  * 广播变量
  *
  * @Author cuihaiyan
  * @Create_Time 2019-10-21 21:11
  */
object Spark09_Oper1_ShareData {

  def main(args: Array[String]): Unit = {
    //设定spark计算框架的运行环境
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WorkCount")
    //创建spark上下文对象
    val sc = new SparkContext(config)


    val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    val i: Int = dataRDD.reduce(_+_)
    println(i)
    //output: 10

    sc.stop()
  }
}
