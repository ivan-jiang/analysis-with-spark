package com.ivan.spark

import org.apache.spark.{SparkConf, SparkContext}

object App {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf(true)
    val sc = new SparkContext("local[2]", "Ivan Spark", config)
    val rawblocks = sc.textFile("file:///D:/dev/projects/data/")
    val head = rawblocks.take(10)
    head.filter(line => !isHeader(line)).foreach(println)
  }

  def isHeader(line: String): Boolean = {
    line.contains("id_1")
  }
}
