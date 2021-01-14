package com.ivan.spark.ch3

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Demo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Ivan Test").master("local[2]").getOrCreate()
    val df: DataFrame = spark.read.json("src/main/resources/people.json")

    import spark.implicits._
    df.printSchema()

    df.select($"name", $"gender" as "sex").withColumn("user", lit(12345L))
      .show()
  }
}
