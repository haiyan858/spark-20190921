package com.atguigu.bigdata.spark.rdd.actions

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * @Author cuihaiyan
  * @Create_Time 2019-10-14 22:43
  */
object Spark07_Oper9_saveAsXXXFile {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    val sc = new SparkContext(config)

    val listRDD: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("c",3)))

    //保存到hdfs下，文本文件
    listRDD.saveAsTextFile("output1")
    listRDD.saveAsSequenceFile("output2")
    listRDD.saveAsObjectFile("output3")

  }
}
