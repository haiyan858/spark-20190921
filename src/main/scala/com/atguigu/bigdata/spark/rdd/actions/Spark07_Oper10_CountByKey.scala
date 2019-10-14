package com.atguigu.bigdata.spark.rdd.actions

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * countByKey()
  *
  * @Author cuihaiyan
  * @Create_Time 2019-10-14 23:18
  */
object Spark07_Oper10_CountByKey {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    val sc = new SparkContext(config)

    val listRDD: RDD[(String, Int)] = sc.parallelize(List(("a",88),("a",88),("b",95),("a",91),("b",93),("a",95),("b",98)))
    val countKeyRDD: collection.Map[String, Long] = listRDD.countByKey()


    /**
      * scala> val rdd = sc.parallelize(List(("a",88),("a",88),("b",95),("a",91),("b",93),("a",95),("b",98)))
      * rdd: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[7] at parallelize at <console>:24
      *
      * scala> rdd.countByKey()
      * res10: scala.collection.Map[String,Long] = Map(a -> 4, b -> 3)
      *
      * scala> rdd.countByValue()
      * res11: scala.collection.Map[(String, Int),Long] = Map((b,93) -> 1, (a,91) -> 1, (a,88) -> 2, (b,95) -> 1, (b,98) -> 1, (a,95) -> 1)
      *
      */


  }
}
