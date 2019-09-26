package com.atguigu.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * @Author cuihaiyan
  * @Create_Time 2019-09-26 13:55
  */
object Spark02_Oper4_flatMap {

  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(config)

    val listRDD: RDD[List[Int]] = sc.parallelize(Array(List(1, 2), List(3, 4)))

    val flatMapRDD: RDD[Int] = listRDD.flatMap(datas => datas)

    flatMapRDD.collect().foreach(println)

  }

}
