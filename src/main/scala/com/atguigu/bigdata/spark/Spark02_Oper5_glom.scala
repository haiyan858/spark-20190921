package com.atguigu.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * @Author cuihaiyan
  * @Create_Time 2019-09-26 17:28
  */
object Spark02_Oper5_glom {

  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val sc = new SparkContext(config)

    //创建一个4个分区的RDD，并将每个分区的数据放到一个数组
    val listRDD: RDD[Int] = sc.makeRDD(1 to 17, 4)

    /*val indexRDD: RDD[(Int, String)] = listRDD.mapPartitionsWithIndex {
      case (index, datas) => {
        datas.map((_, "分区号：" + index))
      }
    }
    indexRDD.collect().foreach(println)*/


  }

}
