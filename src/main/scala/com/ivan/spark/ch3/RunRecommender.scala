package com.ivan.spark.ch3

import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

object RunRecommender {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Music Recommender")
      .master("local[2]").getOrCreate()

    spark.sparkContext.setCheckpointDir("file:///spark")

    val base = "file:///D:/dev/projects/data/c3/"
    val rawUserAartistData = spark.read.textFile(base + "user_artist_data.txt")
    val rawArtistData = spark.read.textFile(base + "artist_data.txt")
    val rawArtistAlias = spark.read.textFile(base + "artist_alias.txt")

    val recommender = new MusicRecommender(spark)
    recommender.prepare(rawUserAartistData, rawArtistData, rawArtistAlias)

    spark.stop()
  }

  class MusicRecommender(private val spark: SparkSession) {

    import spark.implicits._

    def prepare(
                 rawUserArtistData: Dataset[String],
                 rawArtistData: Dataset[String],
                 rawArtistAlias: Dataset[String]
               ): Unit = {
      rawUserArtistData.take(5).foreach(println)

      val userArtistDF = rawUserArtistData.map { line =>
        val Array(user, artist, _*) = line.split(' ')
        (user.toInt, artist.toInt)
      }.toDF("user", "artist")

      userArtistDF.agg(min("user"), max("user"), min("artist"), max("artist")).show()

    }
  }

  private def start(sc: SparkContext): Unit = {
    val rawUserAartistData: RDD[String] = sc.textFile("file:///D:/dev/projects/data/c3/user_artist_data.txt", 10)
    val col0 = rawUserAartistData.map(line => line.split(" ")(0).toDouble).stats()
    val col1 = rawUserAartistData.map((line => line.split(" ")(1).toDouble)).stats()

    val artistById = readArtist(sc).collectAsMap()
    val artistAlias: collection.Map[Int, Int] = readArtistAlias(sc)
    val bArtistById = sc.broadcast(artistById)


    val bArtistAlias = sc.broadcast(artistAlias)
    val trainData = rawUserAartistData.map(line => {
      val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
      val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
      Rating(userID, finalArtistID, count)
    }).persist(StorageLevel.MEMORY_ONLY_SER).setName("trainData")

    val model: MatrixFactorizationModel = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)
    // 打印用户矩阵的第1行(10个隐特征)
    println(model.userFeatures.mapValues(_.mkString(",")).first())

    val userID = 2093760
    val ratingForUser = trainData.filter(r => r.user == userID)
      .map(r => r.product).collect().toSet

    println(s"user ${userID} 过去听过这些艺术家的作品")
    bArtistById.value.filter { case (id, _) => ratingForUser.contains(id) }
      .foreach(println(_))

    println(s"为user ${userID} 推荐的艺术家")
    val recommendations: Array[Rating] = model.recommendProducts(userID, 5)
    val recommendArtists = recommendations.map(r => artistById.getOrElse(r.product, ""))
    println(recommendArtists.mkString(","))
  }


  private def readArtist(sc: SparkContext) = {
    val rawArtistData = sc.textFile("file:///D:/dev/projects/data/c3/artist_data.txt")
    val artistById: RDD[(Int, String)] = rawArtistData.flatMap(line => {
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
    })
    //println(artistById.count())
    artistById
  }

  private def readArtistAlias(sc: SparkContext) = {
    val rawArtistAlias = sc.textFile("file:///D:\\dev\\projects\\data\\c3\\artist_alias.txt")
    val alias: collection.Map[Int, Int] = rawArtistAlias.flatMap(line => {
      val parts = line.split('\t')
      if (parts(0).isEmpty) {
        None
      } else {
        Some(parts(0).toInt, parts(1).toInt)
      }
    }).collectAsMap()
    alias
  }
}
