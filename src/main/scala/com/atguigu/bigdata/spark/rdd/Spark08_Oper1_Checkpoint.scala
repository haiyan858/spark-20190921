package com.atguigu.bigdata.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * RDD checkpoint
  *
  * @Author cuihaiyan
  * @Create_Time 2019-10-16 20:45
  */
object Spark08_Oper1_Checkpoint {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    val mapRDD: RDD[(Int, Int)] = rdd.map((_,1))
    mapRDD.checkpoint()

    val reduceRDD: RDD[(Int, Int)] = mapRDD.reduceByKey(_+_)

    println(reduceRDD.toDebugString)

    //释放资源
    sc.stop()
  }

}
