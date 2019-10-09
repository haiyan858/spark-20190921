package com.atguigu.bigdata.spark.rdd.actions


/**
  * @Author cuihaiyan
  * @Create_Time 2019-10-09 11:03
  */
object Spark06_Oper1_reduce_collect {

  //reduce(f: (T, T) ⇒ T): T
  //先聚合分区内，再聚合分区间

  /**
    * scala> val rdd1 = sc.makeRDD(1 to 10,2)
    * rdd1: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[1] at makeRDD at <console>:24
    * *
    * scala> rdd1.reduce(_+_)
    * res2: Int = 55
    * *
    * scala> rdd1.collect() //收集到Driver端
    * res4: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    *
    */
}
