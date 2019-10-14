package com.atguigu.bigdata.spark.rdd.actions

/**
  *
  *
  * @Author cuihaiyan
  * @Create_Time 2019-10-09 11:55
  */
object Spark06_Oper6_aggregate {

  //aggregate
  //聚合

  //与 aggregateByKey 不同，初始值只设置一次，分区内第一次出现的时候
  //但aggregate 会设置多次，分区内出现的时候都会设置，分区间也会设置

  /**
    *
    * scala> var rdd = sc.makeRDD(1 to 10,2)
    * rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at makeRDD at <console>:24
    *
    * scala> rdd.aggregate(0)(_+_,_+_)
    * res1: Int = 55
    *
    * scala> rdd.aggregate(10)(_+_,_+_)
    * res2: Int = 85
    *
    * 所以多了30
    */
}
