package org.muks.example

import java.nio.file.Paths

object WordCountDfCsv {

  def main(args: Array[String]): Unit = {
    val currentDirectory = new java.io.File(".").getCanonicalPath
    val dataDir = currentDirectory + "/data"
    val inputFile = dataDir + "/Used_Bikes.csv"

    val path = Paths.get(inputFile)
    val fileURI = path.toString;

    println("fullpath:- " + fileURI)


//    /** What is new SparkConf().setMaster("local[*]") */
//    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark DF Word Counter")
//
//    // create Spark context with Spark configuration
//    val sparkContext = new SparkContext(sparkConf.setAppName("SparkDfWordCount"))

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
