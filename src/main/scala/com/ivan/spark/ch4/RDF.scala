package com.ivan.spark.ch4

object RDF {
  def main(args: Array[String]): Unit = {
    // val spark = SparkSession.builder().appName("Spark ch4").master("local[2]").getOrCreate()
    val path = ""
    val colNames = Seq(
      "Elevation", "Aspect", "Slope",
      "Horizontal_Distance_To_Hydrology", "Vertical_Distance_To_Hydrology",
      "Horizontal_Distance_To_Roadways",
      "Hillshade_9am", "Hillshade_Noon", "Hillshade_3pm",
      "Horizontal_Distance_To_Fire_Points"
    ) ++ (
      (0 to 4).map(i => s"Wilderness_Area_$i")
      ) ++ (
      (0 to 40).map(i => s"Soil_Type_$i")
      ) ++ Seq("Cover_Type")

    println(colNames.size)
  }
}
