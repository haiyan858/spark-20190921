package com.atguigu.bigdata.spark.rdd.values

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 双value类型的算子
  *
  * cartesian: 两个RDD的笛卡尔积集
  *
  * @Author cuihaiyan
  * @Create_Time 2019-09-27 20:19
  */
object Spark04_Oper4_cartesian {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(1 to 3)
    //rdd1.glom.collect

    val rdd2 = sc.parallelize(4 to 6)

    val rdd3 = rdd1.cartesian(rdd2)

    rdd3.collect.foreach(println)
    /*
    output:
    (1,4)
    (1,5)
    (1,6)
    (2,4)
    (2,5)
    (2,6)
    (3,4)
    (3,5)
    (3,6)
    */

    //停止：关闭资源
    sc.stop()

  }
}
