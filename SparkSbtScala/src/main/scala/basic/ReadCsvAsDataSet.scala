package org.muks.example
package basic

import org.apache.spark.sql.SparkSession

import java.nio.file.Paths
import scala.reflect.io.File

object ReadCsvAsDataSet {
  def main(args: Array[String]): Unit = {
    val currentDirectory = new java.io.File(".").getCanonicalPath
    val dataDir = currentDirectory + "/data"
    val outDir = currentDirectory + "/output"
    val inputFile = dataDir + "/Used_Bikes.csv"

    val path = Paths.get(inputFile)
    val inputFileURI = path.toString;

    println("input data file:- " + inputFileURI)


    val sparkSession =
      SparkSession
        .builder()
        .master("local")
        .appName("ReadCsvAsDataSet")
        .getOrCreate();

    val bikeDataSet =
      sparkSession
        .read
        .option("header", "true")
        .csv(inputFileURI)

    bikeDataSet.show(10)

    val bikes = bikeDataSet.groupBy("bike_name").count().alias("ByName")
    bikes.show()

    val bikesOutput = outDir + "/bikes"
    val outDirPath = Paths.get(bikesOutput)
    new File(outDirPath.toFile).deleteRecursively()

    bikes
      .coalesce(1)
      .write
      .option("header", "true")
      .format("com.databricks.spark.csv")
      .save(outDir + "/bikes")

    //bikeDataSet.groupBy("bike_name").count().write.format("com.databricks.spark.csv").save(outDir + "/bikes")


  }

}
