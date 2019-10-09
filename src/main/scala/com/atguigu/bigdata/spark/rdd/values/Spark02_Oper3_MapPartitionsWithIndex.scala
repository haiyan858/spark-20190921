package com.atguigu.bigdata.spark.rdd.values

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *
  * @Author cuihaiyan
  * @Create_Time 2019-09-26 13:55
  */
object Spark02_Oper3_MapPartitionsWithIndex {

  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(config)

    //mapPartitionsWithIndex 算子
    val listRDD: RDD[Int] = sc.parallelize(1 to 10,2) //2个分区

    //mapPartitionsWithIndex 分区号
    val indexRDD: RDD[(Int, String)] = listRDD.mapPartitionsWithIndex {
      //模式匹配的写法：多个参数的情况下
      case (num, datas) => {
        datas.map((_, "分区号：" + num))
      }
    }

    indexRDD.collect().foreach(println)
    /*
    (1,分区号：0)
    (2,分区号：0)
    (3,分区号：0)
    (4,分区号：0)
    (5,分区号：0)
    (6,分区号：1)
    (7,分区号：1)
    (8,分区号：1)
    (9,分区号：1)
    (10,分区号：1)
    */

  }

}
