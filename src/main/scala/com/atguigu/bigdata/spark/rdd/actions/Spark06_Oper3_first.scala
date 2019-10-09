package com.atguigu.bigdata.spark.rdd.actions;

/**
 * @Author cuihaiyan
 * @Create_Time 2019-10-09 11:23
 */
object Spark06_Oper3_first {

    //first()
    //返回rdd中第一个元素

    /**
     scala> val rdd = sc.parallelize(1 to 10)
     rdd: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[4] at parallelize at <console>:24

     scala> rdd.count()
     res6: Long = 10

     scala> rdd.first()
     res8: Int = 1

     */
}
