package com.ivan.spark.sql

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator


case class Employee(name: String, salary: Double)

case class Average(var sum: Double, var count: Long)

object MyAverage2 extends Aggregator[Employee, Average, Double] {
  override def zero: Average = Average(0L, 0L)

  override def reduce(buffer: Average, employee: Employee): Average = {
    buffer.sum += employee.salary
    buffer.count += 1
    buffer
  }

  override def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  override def finish(reduction: Average): Double = {
    reduction.sum / reduction.count.toDouble
  }

  override def bufferEncoder: Encoder[Average] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("Spark Sql")
      .getOrCreate()

    import spark.implicits._

    val ds = spark.read.json("D:\\dev\\projects\\my\\spark\\examples\\src\\main\\resources\\employees.json").as[Employee]
    ds.show()

    val averageSalary: TypedColumn[Employee, Double] = MyAverage2.toColumn.name("average_salary")
    val result: Dataset[Double] = ds.select(averageSalary)
    result.show()
  }
}
