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
    val head: Array[String] = rawblocks.take(10).filter(line => !isHeader(line))
    val mds: Array[MatchData] = head.map(parse(_))
    //val counters: Array[NAStatCounter] = statsV2(mds)
    // println("counters")
    //  counters.foreach(println(_))

    val parsed: RDD[MatchData] = rawblocks.filter(line => isHeader(line) == false).map(parse(_))

    parsed.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    parsed.count()

    val grouped = mds.groupBy(md => md.matched)
    //grouped.mapValues(arr => arr.size).foreach(println(_))

    val matchCounts: collection.Map[Boolean, Long] = parsed.map(md => md.matched).countByValue()
    val matchCountSeq = matchCounts.toSeq
    // 默认升序
    //matchCountSeq.sortBy(_._2).foreach(println(_))

    // 降序
    //matchCountSeq.sortBy(_._2).reverse.foreach(println(_))

    // statsV1(parsed)
    val scoresRDD = parsed.map(md => md.scores)
    val counters = NAStatCounter.statsWithMissing(scoresRDD)
    counters.foreach(println(_))

    System.in.read()
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
