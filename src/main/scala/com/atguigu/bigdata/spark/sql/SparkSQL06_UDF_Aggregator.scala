package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * 强类型用户自定义聚合函数：
  * 通过继承Aggregator来实现强类型自定义聚合函数
  *
  * @Author cuihaiyan
  * @Create_Time 2019-11-07 23:33
  */
object SparkSQL06_UDF_Aggregator {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL06_UDF_Aggregator")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    val ds: Dataset[Employee] = spark.read.json("data/employees.json").as[Employee]
    ds.show()
    //+-------+------+
    //|   name|salary|
    //+-------+------+
    //|Michael|  3000|
    //|   Andy|  4500|
    //| Justin|  3500|
    //|  Berta|  4000|
    //+-------+------+

    // Convert the function to a `TypedColumn` and give it a name
    val averageSalary = MyAverage_Aggregator.toColumn.name("average_salary")
    val result = ds.select(averageSalary)
    result.show()
    //+--------------+
    //|average_salary|
    //+--------------+
    //|        3750.0|
    //+--------------+s

  }
}
