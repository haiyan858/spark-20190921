package com.atguigu.bigdata.spark.rdd.values

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * coalesce 缩减分区的数量，可简单理解为合并分区
  *
  * @Author cuihaiyan
  * @Create_Time 2019-09-27 14:19
  */
object Spark03_Oper10_coalesce {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sparkContext.makeRDD(1 to 16, 4)
    println("缩减分区前：" + listRDD.partitions.size)

    val coalesceRDD: RDD[Int] = listRDD.coalesce(3)
    println("缩减分区后：" + coalesceRDD.partitions.size)

    //停止：关闭资源
    sparkContext.stop()

  }
}
