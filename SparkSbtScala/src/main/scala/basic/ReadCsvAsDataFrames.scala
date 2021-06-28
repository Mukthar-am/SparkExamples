package org.muks.example
package basic

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import java.nio.file.Paths

object ReadCsvAsDataFrames {
  def main(args: Array[String]): Unit = {
    val currentDirectory = new java.io.File(".").getCanonicalPath
    val dataDir = currentDirectory + "/data"
    val outDir = currentDirectory + "/output"
    val inputFile = dataDir + "/Used_Bikes.csv"

    val path = Paths.get(inputFile)
    val inputFileURI = path.toString;

    println("inputFileURI:- " + inputFileURI)


    /** What is new SparkConf().setMaster("local[*]") */
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-DF-Word-Counter")

    // create Spark context with Spark configuration
    val sparkContext = new SparkContext(sparkConf)

    // If you already have SparkContext stored in `sc`
    val sparkSession = SparkSession.builder.config(sparkContext.getConf).getOrCreate()

    val csvDf = sparkSession.read.format("csv").option("header", "true").load(inputFileURI)
    csvDf.show(10)

  }

}
