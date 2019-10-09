package com.atguigu.bigdata.spark.rdd.values

/**
  *
  *
  * @Author cuihaiyan
  * @Create_Time 2019-09-26 17:28
  */
object Spark02_Oper5_glom {

  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val sc = new SparkContext(config)

    //创建一个4个分区的RDD，并将一个分区的数据放到一个数组
    //each partition into an array
    //val listRDD: RDD[Int] = sc.makeRDD(1 to 17, 4)
    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,4,5,6,7,7,8), 4)

    /*val indexRDD: RDD[(Int, String)] = listRDD.mapPartitionsWithIndex {
      case (index, datas) => {
        datas.map((_, "分区号：" + index))
      }
    }
    indexRDD.collect().foreach(println)*/

    val glomRDD: RDD[Array[Int]] = listRDD.glom()

    glomRDD.collect().foreach(array => {
      /*for (elem <- array) {
        println(elem)
      }*/
      //println(array.mkString(","))
      //max值
      println(array.max)
    })

  }

}
