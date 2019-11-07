package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 弱类型用户自定义聚合函数：
  * 通过继承UserDefinedAggregateFunction来实现用户自定义聚合函数
  *
  * @Author cuihaiyan
  * @Create_Time 2019-11-07 22:53
  */
object SparkSQL05_UDF {
  def main(args: Array[String]): Unit = {

//      scala> val df = spark.read.json("examples/src/main/resources/people.json")
//      df: org.apache.spark.sql.DataFrame = [age: bigint, name: string]
//
//      scala> spark.udf.register("addName",(x:String)=>"Name"+x)
//      res2: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function1>,StringType,Some(List(StringType)))
//
//      scala> df.createOrReplaceTempView("people")
//      scala> spark.sql("select addName(name),age from people").show
//        +-----------------+----+
//        |UDF:addName(name)| age|
//        +-----------------+----+
//        |      NameMichael|null|
//        |         NameAndy|  30|
//        |       NameJustin|  19|
//        +-----------------+----+

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL05_UDF")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 注册函数
    spark.udf.register("myAverage", MyAverage)

    val df = spark.read.json("data/employees.json")
    df.createOrReplaceTempView("employees")
    df.show()
    //+-------+------+
    //|   name|salary|
    //+-------+------+
    //|Michael|  3000|
    //|   Andy|  4500|
    //| Justin|  3500|
    //|  Berta|  4000|
    //+-------+------+

    val result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees")
    result.show()
    //+--------------+
    //|average_salary|
    //+--------------+
    //|        3750.0|
    //+--------------+

  }

}
