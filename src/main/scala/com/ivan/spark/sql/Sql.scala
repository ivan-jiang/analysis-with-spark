package com.ivan.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Sql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("Spark Sql")
      .getOrCreate()

    inferSchemaProgrammatically(spark)
  }

  private def basic(spark: SparkSession): Unit = {
    import spark.implicits._
    val path = "D:\\dev\\projects\\my\\spark\\examples\\src\\main\\resources\\people.json"
    val people = spark.read.json(path)
    people.printSchema()
    people.show()

    people.select("name").show()
    people.select($"name", $"age" + 5).show()
    people.filter($"age" > 21).show()
    val count: DataFrame = people.groupBy("age").count()
    count.printSchema()

    people.createOrReplaceGlobalTempView("people")
    val sqlDF = spark.sql("select * from global_temp.people")
    sqlDF.show()

    spark.newSession().sql("select * from global_temp.people").show()
  }

  private def dataset(spark: SparkSession): Unit = {
    import spark.implicits._
    val path = "D:\\dev\\projects\\my\\spark\\examples\\src\\main\\resources\\people.json"


    val caseClassDS = Seq(Person("Ivan", 20)).toDS()
    caseClassDS.show()

    val primitiveDS: Dataset[Int] = Seq(1, 2, 3).toDS()
    primitiveDS.printSchema()
    primitiveDS.show()
    primitiveDS.map(_ + 1).show()

    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.printSchema()
    peopleDS.show()
  }

  private def inferSchema(spark: SparkSession): Unit = {
    import spark.implicits._
    val peopelDF = spark.sparkContext.textFile("D:\\dev\\projects\\my\\spark\\examples\\src\\main\\resources\\people.txt")
      .map(_.split(","))
      .map(tokens => Person(tokens(0), tokens(1).trim.toInt))
      .toDF()

    peopelDF.createOrReplaceTempView("people")
    val teenageDF = spark.sql("select * from people where age between 13 and 19")

    teenageDF.map(row => "name:" + row(0)).show()
    teenageDF.map(row => "name:" + row.getAs[String](0)).show()
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    val data = peopelDF.map(row => row.getValuesMap[Any](List("name", "age"))).collect()
    data.foreach(println)
  }

  private def inferSchemaProgrammatically(spark: SparkSession): Unit = {
    import org.apache.spark.sql.types._
    val peopelRDD = spark.sparkContext.textFile("D:\\dev\\projects\\my\\spark\\examples\\src\\main\\resources\\people.txt")
    val schemaString = "name age"
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val structType = StructType(fields)
    val rowRDD: RDD[Row] = peopelRDD.map(_.split(" "))
      .map(tokens => Row(tokens(0), tokens(1).trim))

    val peopleDF = spark.createDataFrame(rowRDD, structType)
    peopleDF.show()

    peopleDF.createOrReplaceTempView("people")
    spark.sql("select * from people").show()
  }

  case class Person(name: String, age: Int)

}
