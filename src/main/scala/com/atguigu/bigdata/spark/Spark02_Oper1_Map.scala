package com.atguigu.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * @Author cuihaiyan
  * @Create_Time 2019-09-26 13:55
  */
object Spark02_Oper1_Map {

  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(config)

    //map 算子
    val listRDD: RDD[Int] = sc.parallelize(1 to 10)
    //val mapRDD: RDD[Int] = listRDD.map(_*2)
    val mapRDD: RDD[Int] = listRDD.map(x => x * 2)
    //mapRDD.foreach(println)
    mapRDD.collect().foreach(println)

  }

}
