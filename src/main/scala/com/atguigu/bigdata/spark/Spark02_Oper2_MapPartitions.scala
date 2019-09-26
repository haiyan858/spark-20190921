package com.atguigu.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * @Author cuihaiyan
  * @Create_Time 2019-09-26 13:55
  */
object Spark02_Oper2_MapPartitions {

  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(config)

    //mapPartitions 算子
    val listRDD: RDD[Int] = sc.parallelize(1 to 10)
    //mapPartitions 可以对一个RDD中的所有的分区进行遍历
    val mapPartitionsRDD: RDD[Int] = listRDD.mapPartitions(datas => {
      datas.map(data => data * 2)
    })

    mapPartitionsRDD.collect().foreach(println)

  }

}
