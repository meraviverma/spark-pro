package com.myexample.util

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

//read a json data and create a dataframe
object Tutorial_3 {

  def main(args:Array[String])={

    System.setProperty("hadoop.home.dir", "D:\\software\\hadoop")

    val sc=SparkSession
      .builder()
      .appName("'")
      .master("local")
      .getOrCreate()

    import sc.implicits._

    sc.sparkContext.setLogLevel("ERROR")

    //reading json
val datajson: DataFrame =sc.read.json("C:\\Users\\rv00451128\\Desktop\\mylearning\\Project\\sparkcassandraexample\\empdata.json")
    datajson.show()

    /*val datapro=sc.sparkContext.textFile("C:\\Users\\rv00451128\\Desktop\\mylearning\\Project\\sparkcassandraexample\\product_revenue.txt")
        .map(_.split(",")).map(e=>Row(e(0),e(1),e(2)))
    */

    val datawithschema: DataFrame =sc.read.option("header",true)
      .csv("C:\\Users\\rv00451128\\Desktop\\mylearning\\Project\\sparkcassandraexample\\product_revenue.txt")

    //val datad=sc.read.load("")
      //.schema()

    datawithschema.show()

    datawithschema.createOrReplaceTempView("product_revenue")
    sc.sql("select product from product_revenue ").show()

    //get list of columns from dataframe
    datawithschema.select("product","revenue").show()

    //get distinct record dataframe
    datawithschema.select("category").distinct().show()


    /*
        +---------+
        | category|
        +---------+
        |   tablet|
        |cellphone|
        +---------+
    */

    //Category wise revenue
    datawithschema.select("category","revenue").groupBy("category").agg("revenue"->"sum").show()

    /*+---------+------------+
    | category|sum(revenue)|
      +---------+------------+
    |   tablet|     20500.0|
      |cellphone|     23000.0|
      +---------+------------+*/

    datawithschema.select("category","revenue").groupBy("category").agg("revenue"->"max","revenue"->"min").show()




  }

}
