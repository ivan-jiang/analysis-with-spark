package com.ivan.spark.ch3

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.recommendation._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.util.Random

object RunRecommender {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Music Recommender")
      .config("spark.sql.crossJoin.enabled", true)
      .master("local[2]").getOrCreate()

    spark.sparkContext.setCheckpointDir("file:///spark")

    val base = "file:///D:/dev/projects/data/c3/"
    val rawUserArtistData: Dataset[String] = spark.read.textFile(base + "user_artist_data.txt")
    val rawArtistData = spark.read.textFile(base + "artist_data.txt")
    val rawArtistAlias = spark.read.textFile(base + "artist_alias.txt")

    val recommender = new MusicRecommender(spark)
    recommender.prepare(rawUserArtistData, rawArtistData, rawArtistAlias)
    recommender.model(rawUserArtistData)

    System.in.read
    spark.stop()
  }

  class MusicRecommender(private val spark: SparkSession) {

    import spark.implicits._

    private var userArtistData: DataFrame = _
    private var artistByID: DataFrame = _
    private var artistAlias: Map[Int, Int] = _

    def prepare(
                 rawUserArtistData: Dataset[String],
                 rawArtistData: Dataset[String],
                 rawArtistAlias: Dataset[String]
               ): Unit = {
      rawUserArtistData.take(5).foreach(println)

      userArtistData = rawUserArtistData.map { line =>
        val Array(user, artist, count) = line.split(' ').map(_.toInt)
        (user, artist, count)
      }.toDF("user", "artist", "count")

      userArtistData.agg(min("user"), max("user"),
        min("artist"), max("artist"),
        min("count"), max("count"), mean("count")).show()

      artistByID = buildArtistByID(rawArtistData)
      artistAlias = buildArtistAlias(rawArtistAlias)

      val (badID, goodID) = artistAlias.head
      artistByID.filter($"id" isin(badID, goodID)).show()
    }

    def makeRecommendation(model: ALSModel, userID: Int, top: Int) = {
      val topRecommend = model.itemFactors
        .select($"id" as ("artist"))
        .withColumn("user", lit(userID))

      model.transform(topRecommend)
        .select("artist", "prediction")
        .orderBy($"prediction".desc)
        .limit(top)
    }

    def model(rawUserArtistData: Dataset[String]) = {
      val bArtistAlias: Broadcast[Map[Int, Int]] = spark.sparkContext.broadcast(artistAlias)
      val trainData = buildCounts(rawUserArtistData, bArtistAlias.value).cache()

      val model = new ALS()
        .setSeed(Random.nextLong())
        .setRank(10)
        .setAlpha(0.1)
        .setRegParam(0.01)
        .setImplicitPrefs(true)
        .setUserCol("user")
        .setItemCol("artist")
        .setRatingCol("count")
        .setPredictionCol("prediction")
        .fit(trainData)

      trainData.unpersist()
      model.userFactors.select("features").show(false)

      val userID = 2093760
      val existingArtistIDs: Array[Int] = trainData.filter($"user" === userID).select("artist").as[Int].collect()
      artistByID.filter($"id" isin (existingArtistIDs: _*)).show()

      val recommendation = makeRecommendation(model, userID, 5)
      recommendation.show()
    }

    private def buildCounts(rawUserArtistData: Dataset[String], artistAlias: Map[Int, Int]) = {
      rawUserArtistData.map { line =>
        var Array(user, artist, count) = line.split(' ').map(_.toInt)
        artist = artistAlias.getOrElse(artist, artist)
        (user, artist, count)
      }.toDF("user", "artist", "count")
    }

    private def buildArtistByID(rawArtistData: Dataset[String]) = {
      rawArtistData.flatMap { line =>
        val (id, name) = line.span(_ != '\t')
        if (name.isEmpty) {
          None
        } else {
          try {
            Some(id.toInt, name.trim)
          } catch {
            case e: NumberFormatException => None
          }
        }
      }.toDF("id", "name")
    }

    def buildArtistAlias(rawArtistAlias: Dataset[String]) = {
      rawArtistAlias.flatMap { line =>
        val Array(artist, alias) = line.split('\t')
        if (artist.isEmpty) {
          None
        } else {
          Some(artist.toInt, alias.toInt)
        }
      }.collect().toMap
    }
  }

}
