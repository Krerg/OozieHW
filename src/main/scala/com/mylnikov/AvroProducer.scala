package com.mylnikov

import org.apache.spark.sql.SparkSession

object AvroProducer {

  def main(args: Array[String]): Unit = {


    val values = List(List("1", "One") ,List("2", "Two") ,List("3", "Three"),List("4","4")).map(x =>(x(0), x(1)))

    val sparkSession = SparkSession.builder()
      .appName("KafkaStreaming")
      //comment this in case deployment
      .master("local[*]")
      .getOrCreate()

    import sparkSession.implicits._
    val df = values.toDF

    df.select("*").write.format("com.databricks.spark.avro").save("./tmp.avro");


    sparkSession.close()

  }

}
