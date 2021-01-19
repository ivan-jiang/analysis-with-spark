package com.ivan.spark.sql

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

case class Order(id: Int, name: String, count: Int, amount: Double)

case class Stats(var sum: Double, var count: Int)

case class Result(average: Double, var orderCount: Int)

object MyAggregator extends Aggregator[Order, Stats, Result] {
  override def zero: Stats = Stats(0, 0)

  override def reduce(stats: Stats, order: Order): Stats = {
    stats.count += order.count
    stats.sum += order.amount
    stats
  }

  override def merge(stats1: Stats, stats2: Stats): Stats = {
    stats1.sum += stats2.sum
    stats1.count += stats2.count
    stats1
  }

  override def finish(stats: Stats): Result = {
    val avg = stats.sum / stats.count
    Result(avg, stats.count)
  }

  override def bufferEncoder: Encoder[Stats] = Encoders.product

  override def outputEncoder: Encoder[Result] = Encoders.product

  def main(args: Array[String]): Unit = {
    val path = "src/main/resources/order.txt"
    val spark = SparkSession.builder().appName("Sql Sample")
      .master("local[2]").getOrCreate()
    import spark.implicits._
    val df = spark.read.textFile(path).map { line =>
      val Array(id, name, count, amount) = line.split(",")
      Order(id.toInt, name.trim, count.toInt, amount.toDouble)
    }
    df.show()

    val statsColumn = MyAggregator.toColumn.name("stats")
    val stats = df.select(statsColumn)
    stats.printSchema()
  }
}
