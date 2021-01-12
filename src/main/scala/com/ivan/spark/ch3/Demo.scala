package com.ivan.spark.ch3

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Demo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Music Recommender")
      .master("local[2]").getOrCreate()
    val data: RDD[String] = spark.sparkContext.parallelize(Array("a_b", "c_d", "e_f"))
    data.foreach(println)
    data.map { s =>
      val Array(x, y) = s.split('_')
      (x, y)
    }.foreach(println)

    data.flatMap { line =>
      val d = line.split('_')
      d

    }.foreach(println)
  }
}
