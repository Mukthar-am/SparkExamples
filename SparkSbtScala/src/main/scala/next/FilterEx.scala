package org.muks.example
package next



import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.{SparkConf, SparkContext}

object FilterEx {
  def main(args: Array[String]): Unit = {
    val currentDirectory = new java.io.File(".").getCanonicalPath
    val dataDir = currentDirectory + "/data"
    val studentsFile = dataDir + "/Used_Bikes.csv"

    /** What is new SparkConf().setMaster("local[*]") */
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("FilterBy")
    sparkConf.set("spark.executor.memory", "4g")
    sparkConf.set("spark.driver.memory", "4g")
    sparkConf.set("spark.cores.max", "2")
    sparkConf.set("spark.task.maxDirectResultSize", "10g")
    sparkConf.set("spark.sql.crossJoin.enabled", "true")
    sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")


    // create Spark context with Spark configuration
    val sparkContext = new SparkContext(sparkConf)

    // If you already have SparkContext stored in `sc`
    val sparkSession = SparkSession.builder.config(sparkContext.getConf).getOrCreate()


    val bikesDF = sparkSession.read.format("csv").option("header", "true").load(studentsFile)
    bikesDF.show(20)

    // filter based on multiple col conditions and boolean value
    bikesDF
      .filter(
        col("city").isin("Bangalore".toLowerCase, "Delhi")
          and
          col("age").lt("5.0")
      )
      .show()

  }

}
