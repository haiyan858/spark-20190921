package com.atguigu.bigdata.spark.sql

/**
  *
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


  }

}
