package com.atguigu.bigdata.spark.rdd.values

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * sortBy 按照给定的规则排序， 默认正序
  *
  * @Author cuihaiyan
  * @Create_Time 2019-09-27 20:19
  */
object Spark03_Oper11_sortBy {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sparkContext.makeRDD(1 to 16, 4)
    //listRDD.sortBy(x=>x).collect.foreach(println)
    //对3取余数的大小排序
    listRDD.sortBy(x => x % 3).collect.foreach(println)

    //停止：关闭资源
    sparkContext.stop()

  }
}
