package com.atguigu.bigdata.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//import scala.util.parsing.json.JSON


/**
  * 数据读取与保存：Json文件
  *
  * @Author cuihaiyan
  * @Create_Time 2019-10-16 21:15
  */
object Spark08_Oper2_JSON {


  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val json = sc.textFile("in/user.json")
    //val result = json.map(JSON.parseFull)

    //result.foreach(println)

    //释放资源
    sc.stop()


    /*
    scala> val json = sc.textFile("examples/src/main/resources/people.json")
    json: org.apache.spark.rdd.RDD[String] = examples/src/main/resources/people.json MapPartitionsRDD[3] at textFile at <console>:24

    scala> import scala.util.parsing.json.JSON
    import scala.util.parsing.json.JSON

    scala> val result = json.map(JSON.parseFull)
    warning: there was one deprecation warning; re-run with -deprecation for details
    result: org.apache.spark.rdd.RDD[Option[Any]] = MapPartitionsRDD[4] at map at <console>:26

    scala> result.foreach(println)
    Some(Map(name -> Michael))
    Some(Map(name -> Justin, age -> 19.0))
    Some(Map(name -> Andy, age -> 30.0))
    */

  }

}
