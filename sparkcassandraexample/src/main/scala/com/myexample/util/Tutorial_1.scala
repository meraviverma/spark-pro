package com.myexample.util

import org.apache.spark.sql.{SQLContext, SparkSession}
import com.datastax.spark.connector._

object Tutorial_1 {
  case class Person(name: String, surname: String, children: Int)
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\software\\hadoop")

    val sc=SparkSession
      .builder()
      .appName("sparkcassandra")
      .master("local")
      .getOrCreate()

    import sc.implicits._

    sc.sparkContext.setLogLevel("ERROR")

    //sc.conf.set("spark.cassandra.connection.host","127.0.0.1:9042")


    val test_spark_rdd=sc.sparkContext.cassandraTable("hr","emp")
    println(test_spark_rdd.collect().mkString(","))

    /*SaveMode.Append will update it
      SaveMode.Overwrite will truncate and insert (but it requires option "confirm.truncate" -> "true")
    SaveMode.Ignore will not perform any action on existing table
    SaveMode.ErrorIfExists (default) will throw the following exception*/

    //Append Mode
   val newNames = sc.sparkContext.parallelize(Seq(Person("Eleni", "Garcia", 1), Person("Galina", "Xinn", 5), Person("Carlo", "Tran", 1))).toDS
    newNames.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "people", "keyspace" -> "hr")).
      mode(org.apache.spark.sql.SaveMode.Append).save

    //Overwrite Mode
    val newNamesover = sc.sparkContext.parallelize(Seq(Person("Eleni", "Garcia", 1), Person("Galina", "Xin", 2), Person("Carlo", "Tran", 1))).toDS
    newNamesover.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "people", "keyspace" -> "hr", "confirm.truncate" -> "true")).
      mode(org.apache.spark.sql.SaveMode.Overwrite).save

    //Load cassandra table
    val tabletoload=sc.read.format("org.apache.spark.sql.cassandra")
      .options(Map("table"->"people","keyspace"->"hr"))
      .load()

   tabletoload.createOrReplaceTempView("abc")

    sc.sql("select * from abc").show()
  }
}
