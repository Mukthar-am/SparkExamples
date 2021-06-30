package org.muks.example
package next


import next.CsvAggregationsDF.{deptFile, studentsFile}

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

// https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-joins.html

/**
 * Ex:
 *  - This demonstrates how to join 2 CSVs based on a column
 *  - Drop some columns from either of the CSVs/DataFrames
 */

object JoinEx {
  def main(args: Array[String]): Unit = {

    val currentDirectory = new java.io.File(".").getCanonicalPath
    val dataDir = currentDirectory + "/data"
    studentsFile = dataDir + "/Used_Bikes.csv"
    deptFile = dataDir + "/brand_metadata.csv"

    //    studentsFile = dataDir + "/students.csv"
    //    deptFile = dataDir + "/dept.csv"


    /** What is new SparkConf().setMaster("local[*]") */
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("CsvAggregationsAsDF")
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


    val studentsDF = sparkSession.read.format("csv").option("header", "true").load(studentsFile.toString)
    val deptDF = sparkSession.read.format("csv").option("header", "true").load(deptFile.toString)

    studentsDF.show(10)
    deptDF.show()


    //    val inner_df = studentsDF.join(deptDF, studentsDF("id") === deptDF("id")).drop(deptDF("id"))
    //    inner_df.show()


    val bikesDataJoinedDF =
      studentsDF
        .join(deptDF, studentsDF("brand") === deptDF("brand"))
        .drop(studentsDF("power"))
        .drop(deptDF("brand"))

    bikesDataJoinedDF.show()

  }

}
