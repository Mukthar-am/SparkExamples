package org.muks.example
package basic

import java.nio.file.Paths

object WordCountDfCsv {

  def main(args: Array[String]): Unit = {
    val currentDirectory = new java.io.File(".").getCanonicalPath
    val dataDir = currentDirectory + "/data"
    val inputFile = dataDir + "/Used_Bikes.csv"

    val path = Paths.get(inputFile)
    val fileURI = path.toString;

    println("fullpath:- " + fileURI)


    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;

    val df = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load(fileURI)
    df.printSchema()


  }

}
