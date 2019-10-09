package com.atguigu.bigdata.spark.rdd.values

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 双value类型的算子
  *
  * subtract: 两个RDD的差集
  * 计算差，去除两个RDD中相同的元素，不同的RDD将保留下来
  *
  * @Author cuihaiyan
  * @Create_Time 2019-09-27 20:19
  */
object Spark04_Oper2_subtract {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(1 to 5)
    //rdd1.glom.collect

    val rdd2 = sc.parallelize(5 to 10)

    val rdd3 = rdd1.subtract(rdd2)

    rdd3.collect.foreach(println)

    //停止：关闭资源
    sc.stop()

  }
}
