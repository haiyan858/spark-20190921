package com.atguigu.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * @Author cuihaiyan
  * @Create_Time 2019-09-26 09:51
  */
object Spark01_RDD {

  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(config)

    //创建RDD

    // 1) 从内存中创建 makeRDD
    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4)) //传入seq
    val value2: RDD[String] = sc.makeRDD(Array("1", "2"))
    listRDD.collect().foreach(println)

    // 2) 从内存中创建
    var arrayRDD: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4))

    // 3) 从外部存储创建
    // 默认情况下，读取项目路径，也可以读取其他路径， 比如HDFS://
    // 默认从文件读取的数据都是字符串类型
    //val fileRDD: RDD[String] = sc.textFile("in")
    val fileRDD: RDD[String] = sc.textFile("in", 3)

    //将RDD数据保存到文件中
    listRDD.saveAsTextFile("output") //并行度

  }
}
