package com.ivan.spark.ch2

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.StatCounter
import org.apache.spark.{SparkConf, SparkContext}

import java.lang.Double.isNaN
import scala.collection.immutable

object App {
  def main(args: Array[String]): Unit = {
    start()
  }

  def start(): Unit = {
    val config = new SparkConf(true)
    val sc = new SparkContext("local[2]", "Ivan Spark", config)
    val rawblocks: RDD[String] = sc.textFile("file:///D:/dev/projects/data/")
    //val head: Array[String] = rawblocks.take(10).filter(line => !isHeader(line))
    val parsed: RDD[MatchData] = rawblocks.filter(line => isHeader(line) == false).map(parse(_))

    parsed.persist(StorageLevel.MEMORY_ONLY_SER)
    parsed.count()

    val countersMatched = NAStatCounter.statsWithMissingV2(parsed.filter(_.matched).map(_.scores))
    val countersNotMatched = NAStatCounter.statsWithMissingV2(parsed.filter(!_.matched).map(_.scores))

    countersMatched.zip(countersNotMatched).map { case (x, y) =>
      (x.missing + y.missing, x.stats.mean - y.stats.mean)
    }.foreach(println)
    val scored = scoring(parsed)
    scored.filter(s => s.score >= 4.3).map(s => s.md.matched).countByValue().foreach(println)

    System.in.read()
  }

  private def toScore(v: Double) = if (Double.NaN.equals(v)) 0 else v

  case class Scored(md: MatchData, score: Double)

  private def scoring(data: RDD[MatchData]) = {
    data.map(md => {
      val score = Array(2, 5, 6, 7, 8).map(i => toScore(md.scores(i))).sum
      Scored(md, score)
    })
  }

  // 统计每个score的概要信息
  def statsV1(parsed: RDD[MatchData]): Unit = {
    val stats: immutable.Seq[StatCounter] = (0 until 9).map(i => parsed.map(md => md.scores(i)).filter(!isNaN(_)).stats())
    for (elem <- stats) {
      println(elem)
    }

  }

  def isHeader(line: String): Boolean = {
    line.contains("id_1")
  }

  def parse(line: String): MatchData = {
    val pieces: Array[String] = line.split(",")
    val id1 = pieces(0).toInt
    val id2 = pieces(1).toInt
    val matched = pieces(11).toBoolean
    val scores = pieces.slice(2, 11).map(toDouble(_))
    MatchData(id1, id2, scores, matched)
  }

  def toDouble(v: String) = {
    if ("?".equals(v)) Double.NaN else v.toDouble
  }

  case class MatchData(id1: Int, id2: Int, scores: Array[Double], matched: Boolean)

}
