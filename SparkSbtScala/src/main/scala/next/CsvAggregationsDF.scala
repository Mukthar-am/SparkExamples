package org.muks.example
package next

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import java.nio.file.{Path, Paths}


object CsvAggregationsDF {
  var studentsFile: String = null
  var inputFilePath: Path = null
  var deptFile: String = null


  def main(args: Array[String]): Unit = {

    getInputFile()
    println("Input File:- " + inputFilePath.toString)


    /** What is new SparkConf().setMaster("local[*]") */
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("CsvAggregationsAsDF")
    sparkConf.set("spark.executor.memory", "4g")
    sparkConf.set("spark.driver.memory", "4g")
    sparkConf.set("spark.cores.max", "2")
    sparkConf.set("spark.task.maxDirectResultSize", "10g")
    sparkConf.set("spark.sql.crossJoin.enabled", "true")
    sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10L * 1024 * 1024")


    // create Spark context with Spark configuration
    val sparkContext = new SparkContext(sparkConf)

    // If you already have SparkContext stored in `sc`
    val sparkSession = SparkSession.builder.config(sparkContext.getConf).getOrCreate()


    val inputDF = sparkSession.read.format("csv").option("header", "true").load(inputFilePath.toString)
//    inputDF.show(10)

    val countryDF = sparkSession.read.format("csv").option("header", "true").load(deptFile)
//    countryDF.show(10)


    val joinedDF = inputDF.join(countryDF, inputDF("brand")===countryDF("brand"))



//    val df_as_input = inputDF.as("input")
//    val df_as_profile = countryDF.as("dfprofile")
//
//    val joined_df = df_as_input.join(
//      df_as_profile
//      , col("input.brand") === col("dfprofile.brand")
//      , "inner").show(false)


//    inputDF.as("input")
//    countryDF.as("country")
//
//    inputDF
//      .join(countryDF, $"input.brand" === $"country.brand")
//      .select($"matches.matchId" as "matchId", $"matches.player1" as "player1", $"matches.player2" as "player2", $"players.birthYear" as "player1BirthYear")
//      .join(playersDf, $"player2" === $"players.player")
//      .select($"matchId" as "MatchID", $"player1" as "Player1", $"player2" as "Player2", $"player1BirthYear" as "BYear_P1", $"players.birthYear" as "BYear_P2")
//      .withColumn("Diff", abs('BYear_P2.minus('BYear_P1)))
//      .show()



//    val joinedDF = inputDF.join(countryDF).where(inputDF("brand") === countryDF("brand"))
//    joinedDF.show()


    //    val bikesByCity = inputDF.groupBy("city").count()
    //
    //    /** rename a column header */
    //    bikesByCity
    //      .withColumnRenamed("city", "vehicle-city")
    //      .withColumnRenamed("count","total-vehicles")
    //      .show(10)
//
//    inputDF.groupBy("brand").count().show(10)
//

  }


  def getInputFile() = {
    val currentDirectory = new java.io.File(".").getCanonicalPath
    val dataDir = currentDirectory + "/data"
    studentsFile = dataDir + "/Used_Bikes.csv"
    deptFile = dataDir + "/brand_metadata.csv"

    inputFilePath = Paths.get(studentsFile)
  }


}