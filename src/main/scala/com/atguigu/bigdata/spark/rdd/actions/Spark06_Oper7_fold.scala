package com.atguigu.bigdata.spark.rdd.actions

/**
  *
  *
  * @Author cuihaiyan
  * @Create_Time 2019-10-14 22:28
  */
object Spark06_Oper7_fold {

  //fold 折叠操作，aggregate的简化版操作

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
    * scala> rdd.fold(0)(_+_)
    * res3: Int = 55
    *
    * scala> rdd.fold(10)(_+_)
    * res4: Int = 85
    *
    */
}
