package com.ivan.spark.ch3

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.recommendation._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer
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
    recommender.evaluate(rawArtistAlias)

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
        .select($"id" as "artist")
        .withColumn("user", lit(userID))

      model.transform(topRecommend)
        .select("artist", "prediction")
        .orderBy($"prediction".desc)
        .limit(top)
    }

    def model(rawUserArtistData: Dataset[String]) = {
      val bArtistAlias = spark.sparkContext.broadcast(artistAlias)
      val trainData = buildCounts(rawUserArtistData, bArtistAlias.value).cache()

      val model = new ALS()
        .setSeed(Random.nextLong())
        .setRank(10)
        .setMaxIter(5)
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
      // 运算符:_*把数组转化为可变参数
      println(s"用户$userID 过去听过这些歌手的音乐")
      artistByID.filter($"id" isin (existingArtistIDs: _*)).show()

      val topRecommendations = makeRecommendation(model, userID, 5)
      val topArtist: Array[Int] = topRecommendations.select("artist").as[Int].collect()
      println(s"为用户$userID 这些歌手")
      artistByID.filter($"id" isin (topArtist: _*)).show()

      model.userFactors.unpersist()
      model.itemFactors.unpersist()
    }

    def evaluate(rawUserArtistData: Dataset[String]): Unit = {
      val bArtistAlias = spark.sparkContext.broadcast(artistAlias)
      val allData = buildCounts(rawUserArtistData, bArtistAlias.value)
      val Array(trainData, cvData) = allData.randomSplit(Array(0.9, 0.1))
      trainData.cache()
      cvData.cache()

      val allArtistIDs: Array[Int] = allData.select("artist").as[Int].distinct().collect()
      val bAllArtistIDs = spark.sparkContext.broadcast(allArtistIDs)

      val predictFunction = predictMostListened(trainData) _
      val mostListenedAUC = meanAUC(cvData, bAllArtistIDs, predictFunction)
      println(mostListenedAUC)

      val evaluations =
        for (rank <- Seq(5, 30);
             regParam <- Seq(1.0, 0.0001);
             alpha <- Seq(1.0, 40.0))
          yield {
            val model = new ALS().
              setSeed(Random.nextLong()).
              setImplicitPrefs(true).
              setRank(rank).setRegParam(regParam).
              setAlpha(alpha).setMaxIter(20).
              setUserCol("user").setItemCol("artist").
              setRatingCol("count").setPredictionCol("prediction").
              fit(trainData)

            val auc = meanAUC(cvData, bAllArtistIDs, model.transform)

            model.userFactors.unpersist()
            model.itemFactors.unpersist()

            (auc, (rank, regParam, alpha))
          }

      evaluations.sorted.reverse.foreach(println)

      trainData.unpersist()
      cvData.unpersist()
    }

    private def meanAUC(positiveData: DataFrame,
                        bAllArtistIDs: Broadcast[Array[Int]],
                        predictFunction: (DataFrame => DataFrame)) = {
      // 计算每个用户的AUC,然后求均值
      val positivePredictions = predictFunction(positiveData.select("user", "artist"))
        .withColumnRenamed("prediction", "positivePrediction")

      // Create a set of "negative" products for each user. These are randomly chosen
      // from among all of the other artists, excluding those that are "positive" for the user.
      val negativeData = positiveData.select("user", "artist").as[(Int, Int)]
        .groupByKey { case (user, _) => user }
        .flatMapGroups { case (userID, userIDAndPosartistIDs) =>
          val random = new Random
          val posItemIDSet = userIDAndPosartistIDs.map { case (_, artist) => artist }.toSet
          val negative = new ArrayBuffer[Int]()
          val allArtistIDs = bAllArtistIDs.value
          var i = 0
          while (i < allArtistIDs.length && negative.size < posItemIDSet.size) {
            val artistID = allArtistIDs(random.nextInt(allArtistIDs.length))
            if (!posItemIDSet.contains(artistID)) {
              negative += artistID
            }
            // 等价于i++，scala没有++运算符
            i += 1
          }
          negative.map(artistID => (userID, artistID))
        }.toDF("user", "artist")

      val negativePredictions = predictFunction(negativeData).
        withColumnRenamed("prediction", "negativePrediction")

      val joinedPredictions = positivePredictions.join(negativePredictions, "user")
        .select("user", "positivePrediction", "negativePrediction").cache()
      // Count the number of pairs per user
      val allCounts = joinedPredictions.
        groupBy("user").agg(count(lit("1")).as("total")).
        select("user", "total")
      // Count the number of correctly ordered pairs per user
      val correctCounts = joinedPredictions.
        filter($"positivePrediction" > $"negativePrediction").
        groupBy("user").agg(count("user").as("correct")).
        select("user", "correct")

      // Combine these, compute their ratio, and average over all users
      val meanAUC = allCounts.join(correctCounts, Seq("user"), "left_outer").
        select($"user", (coalesce($"correct", lit(0)) / $"total").as("auc")).
        agg(mean("auc")).
        as[Double].first()

      joinedPredictions.unpersist()

      meanAUC
    }

    private def predictMostListened(trainData: Dataset[Row])(allData: DataFrame) = {
      val listenCounts = trainData.groupBy("artist")
        .agg(sum("count") as "prediction")
        .select("artist", "prediction")

      allData.join(listenCounts, Seq("artist"), "left_outer")
        .select("user", "artist", "prediction")
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

    private def buildArtistAlias(rawArtistAlias: Dataset[String]) = {
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
