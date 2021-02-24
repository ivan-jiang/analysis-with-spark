package com.ivan.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]")
      .config("spark.scheduler.mode", "fair")
      .config("spark.scheduler.allocation.file", "D:\\dev\\projects\\my\\analysis-with-spark\\src\\main\\resources\\fairscheduler.xml")
      .appName("Ivan App")
      .getOrCreate()
    val rdd = spark.sparkContext.parallelize(Seq("hello", "world", "spark", "apache", "world", "spark"))
    val wordCount = rdd.map((w => (w, 1)))
    wordCount.groupByKey().map { case (w, seq) => (w, seq.size) }.foreach(println)
    wordCount.reduceByKey((x, y) => x + y).foreach(println)

    val DBName = Array((1, "Spark"), (2, "Hadoop"), (3, "Kylin"), (4, "Flink"), (3, "Flink"))
    val numType = Array((1, "String"), (2, "int"), (3, "byte"), (5, "float"), (1, "34"), (2, "45"), (3, "75"))

    val names: RDD[(Int, String)] = spark.sparkContext.parallelize(DBName)
    val types: RDD[(Int, String)] = spark.sparkContext.parallelize(numType)
  }
}
