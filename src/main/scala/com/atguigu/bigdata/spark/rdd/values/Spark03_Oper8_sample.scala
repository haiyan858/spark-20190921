package com.atguigu.bigdata.spark.rdd.values

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * sample 随机抽样
  *
  * @Author cuihaiyan
  * @Create_Time 2019-09-27 11:48
  */
object Spark03_Oper8_sample {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sparkContext = new SparkContext(conf)

    //创建一个RDD，从中选择放回和不放回抽样
    val listRDD: RDD[Int] = sparkContext.makeRDD(1 to 10)

    // 不放回
    // seed 当前时间戳
    //val sampleRDD: RDD[Int] = listRDD.sample(true,0.4,2)
    val sampleRDD: RDD[Int] = listRDD.sample(true,0.4,System.currentTimeMillis())

    sampleRDD.collect().foreach(println)
  }

}
