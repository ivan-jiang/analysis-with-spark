package com.ivan.spark.ch4

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random

object RDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark ch4").master("local[2]").getOrCreate()
    import spark.implicits._
    val path = "D:\\dev\\projects\\data\\analysis-with-spark\\c4\\covtype\\covtype.data"
    val dataWithoutHeader = spark.read
      .option("inferSchema", true)
      .option("header", false)
      .csv(path)
    val colNames = Seq(
      "Elevation", "Aspect", "Slope",
      "Horizontal_Distance_To_Hydrology", "Vertical_Distance_To_Hydrology",
      "Horizontal_Distance_To_Roadways",
      "Hillshade_9am", "Hillshade_Noon", "Hillshade_3pm",
      "Horizontal_Distance_To_Fire_Points"
    ) ++ (
      (0 until 4).map(i => s"Wilderness_Area_$i")
      ) ++ (
      (0 until 40).map(i => s"Soil_Type_$i")
      ) ++ Seq("Cover_Type")

    val data = dataWithoutHeader.toDF(colNames: _*)
      .withColumn("Cover_Type", $"Cover_Type".cast(DoubleType))
    // data.printSchema()
    //data.head()
    // Split into 90% train (+ CV), 10% test
    val Array(trainData, testData) = data.randomSplit(Array(0.9, 0.1))
    trainData.cache()
    testData.cache()
    //trainData.select("Elevation", "Cover_Type").show(100)
    val runRDF = new RunRDF(spark)
    runRDF.simpleDecisionTree(trainData, testData)
    runRDF.randomClassifier(trainData, testData)

    trainData.unpersist()
    testData.unpersist()
  }
}

class RunRDF(private val spark: SparkSession) {

  import spark.implicits._

  def simpleDecisionTree(trainData: DataFrame, testData: DataFrame): Unit = {
    val inputCols = trainData.columns.filter(_ != "Cover_Type")
    val assembler = new VectorAssembler()
      .setInputCols(inputCols)
      .setOutputCol("featureVector")

    val assembledTrainData = assembler.transform(trainData)
    //assembledTrainData.select("featureVector").show(5, truncate = false)
    val classifier = new DecisionTreeClassifier()
      .setSeed(Random.nextLong())
      .setLabelCol("Cover_Type")
      .setFeaturesCol("featureVector")
      .setPredictionCol("prediction")

    val model = classifier.fit(assembledTrainData)
    println(model.toDebugString)
    model.featureImportances.toArray.zip(inputCols)
      .sorted.reverse.foreach(println)

    val predictions = model.transform(assembledTrainData)
    println("predictions_")
    predictions.select("Cover_Type", "probability", "prediction").show(5, truncate = false)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("Cover_Type")
      .setPredictionCol("prediction")

    val accuracy = evaluator.setMetricName("accuracy").evaluate(predictions)
    val f1 = evaluator.setMetricName("f1").evaluate(predictions)

    println("accuracy_1 " + accuracy)
    println("f1 " + f1)

    val predictionRDD = predictions.select("prediction", "Cover_Type")
      .as[(Double, Double)].rdd
    val multiclassMetrics = new MulticlassMetrics(predictionRDD)
    println(multiclassMetrics.confusionMatrix)

    println("confusionMatrix_")
    val confusionMatrix = predictions.groupBy("Cover_Type")
      .pivot("prediction", (0 to 7))
      .count()
      .na.fill(0.0)
      .orderBy("Cover_Type")
    confusionMatrix.show()
  }

  def classProbabilities(data: DataFrame) = {
    val total = data.count()
    val df = data.groupBy("Cover_Type").count()
    df.printSchema()
    val res = df.orderBy("Cover_Type")
      .select("count").as[Double]
      .map(_ / total)
      .collect()
    res.foreach(println)
    res
  }

  def randomClassifier(trainData: DataFrame, testData: DataFrame): Unit = {
    val trainPriorProbabilities = classProbabilities(trainData)
    val testPriorProbabilities = classProbabilities(testData)
    val accuracy = trainPriorProbabilities.zip(testPriorProbabilities)
      .map {
        case (trainProb, testProb) => trainProb * testProb
      }.sum
    println("accuracy_2 " + accuracy)
  }
}
