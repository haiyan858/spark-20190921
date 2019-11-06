package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * spark SQL
  *
  * @Author cuihaiyan
  * @Create_Time 2019-11-04 21:50
  */
object SparkSQL01_Demo {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // Display the content of the DataFrame to stdout
    val df: DataFrame = spark.read.json("data/people.json")
    df.show()
    /*
    +----+-------+
    | age|   name|
    +----+-------+
    |null|Michael|
    |  30|   Andy|
    |  19| Justin|
    +----+-------+
    */


    //df.filter($"age">21)

    spark.stop()
  }
}
