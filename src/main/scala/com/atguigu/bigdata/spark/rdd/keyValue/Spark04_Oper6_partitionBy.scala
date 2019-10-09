package com.atguigu.bigdata.spark.rdd.keyValue

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * 自定义分区器
  *
  * key-value类型的算子
  *
  * partitionBy: Return a copy of the RDD partitioned using the specified partitioner.
  * 对 pairRDD 进行分区操作，如果原有的pairRDD 和现有的 pairRDD 是一致的话，就不进行分区，
  * 否则会生成shuffleRDD， 即会产生shuffle过程
  *
  * @Author cuihaiyan
  * @Create_Time 2019-09-27 23:19
  */
object Spark04_Oper6_partitionBy {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array((1, "aaa"), (2, "bbb"), (3, "ccc"), (4, "ddd")), 4)
    //res31: Array[(Int, String)] = Array((1,aaa), (2,bbb), (3,ccc), (4,ddd))

    //val rdd2 = rdd.partitionBy(new org.apache.spark.HashPartitioner(2))
    val rdd2 = rdd.partitionBy(new MyPartitioner(2))

    rdd2.saveAsTextFile("output-partition")

    //res34: Array[Array[(Int, String)]] = Array(Array((2,bbb), (4,ddd)), Array((1,aaa), (3,ccc)))
    //rdd2.collect.foreach(println)
    /*
    output:
    (2,bbb)
    (4,ddd)
    (1,aaa)
    (3,ccc)
    */

    //停止：关闭资源
    sc.stop()

  }
}

/**
  * 声明分区器
  * 继承抽象类 Partitioner
  *
  * @param partitions
  */
class MyPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    1
  }
}