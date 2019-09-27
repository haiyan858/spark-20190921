package com.atguigu.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * @Author cuihaiyan
  * @Create_Time 2019-09-27 14:19
  */
object Spark03_Oper9_distinct {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sparkContext.makeRDD(List(1,1,2,2,4,5,5,6))

    val distinctRDD: RDD[Int] = listRDD.distinct()

    //distinctRDD.collect().foreach(println)
    distinctRDD.saveAsTextFile("output-distinct")

  }
}
