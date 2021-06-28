package org.muks.example
package next

import utils.Cluster

import java.nio.file.Paths

object CsvAggregationsDF {

  def getInputFile(): String = {
    val currentDirectory = new java.io.File(".").getCanonicalPath
    val dataDir = currentDirectory + "/data"
    val outDir = currentDirectory + "/output"
    val inputFile = dataDir + "/Used_Bikes.csv"

    val path = Paths.get(inputFile)
    return path.toString;

  }


  def main(args: Array[String]): Unit = {

    val inputFilePath = getInputFile()
    println("Input File:- " + inputFilePath)

    val sparkSession = new Cluster()
      .setSparkConf("local[*]", "CsvAggregationsAsDF")
      .setSparkContext()
      .getSparkSession()


    val df = sparkSession.read.format("csv").option("header", "true").load(inputFilePath)
    df.show(10)

    val bikesByCity = df.groupBy("city").count()

    /** rename a column header */
    bikesByCity
      .withColumnRenamed("city", "vehicle-city")
      .withColumnRenamed("count","total-vehicles")
      .show(10)

  }


}