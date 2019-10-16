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

    //设置检查点的保存目录
    //Checkpoint directory has not been set in the SparkContext
    sc.setCheckpointDir("cp")

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    val mapRDD: RDD[(Int, Int)] = rdd.map((_,1))

    //checkpoint
    mapRDD.checkpoint()

    val reduceRDD: RDD[(Int, Int)] = mapRDD.reduceByKey(_+_)

    reduceRDD.foreach(println)
    println(reduceRDD.toDebugString)
    /*
    (4) ShuffledRDD[2] at reduceByKey at Spark08_Oper1_Checkpoint.scala:22 []
    +-(4) MapPartitionsRDD[1] at map at Spark08_Oper1_Checkpoint.scala:20 []
    |  ParallelCollectionRDD[0] at makeRDD at Spark08_Oper1_Checkpoint.scala:18 []
    */

    /*
    (4) ShuffledRDD[2] at reduceByKey at Spark08_Oper1_Checkpoint.scala:29 []
    +-(4) MapPartitionsRDD[1] at map at Spark08_Oper1_Checkpoint.scala:24 []
    |  ReliableCheckpointRDD[3] at foreach at Spark08_Oper1_Checkpoint.scala:31 []
    */
    //释放资源
    sc.stop()
  }

}
