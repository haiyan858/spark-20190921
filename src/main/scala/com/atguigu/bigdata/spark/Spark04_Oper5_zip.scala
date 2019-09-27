package com.atguigu.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 双value类型的算子
  *
  * zip: 两个RDD 组合为key-value形式的RDD，默认两个RDD的partition数量以及元素数量都相同，否则会抛出异常
  *
  * @Author cuihaiyan
  * @Create_Time 2019-09-27 20:19
  */
object Spark04_Oper5_zip {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(1 to 3)
    //rdd1.glom.collect

    val rdd2 = sc.parallelize(4 to 6)

    val rdd3 = rdd1.zip(rdd2)

    rdd3.collect.foreach(println)
    /*
    output:
    (1,4)
    (2,5)
    (3,6)
    */

    //停止：关闭资源
    sc.stop()

  }
}
