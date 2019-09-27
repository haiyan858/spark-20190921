package com.atguigu.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * @Author cuihaiyan
  * @Create_Time 2019-09-27 11:35
  */
object Spark03_Oper7_filter {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sparkContext.makeRDD(List(1, 2, 3, 4))

    //按照指定规则过滤
    val filterRDD: RDD[Int] = listRDD.filter(x => x % 2 == 0)

    filterRDD.collect().foreach(println)

  }
}
