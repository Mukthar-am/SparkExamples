package org.muks.example

import org.apache.spark.{SparkConf, SparkContext}

import java.nio.file.Paths
import scala.reflect.io.File

/**
 *
 * *
 * What is new SparkConf().setMaster("local[*]")
 * - local : Run Spark locally with one worker thread (i.e. no parallelism at all).
 * - local[K] : Run Spark locally with K worker threads (ideally, set this to the number of cores on your machine).
 * - local[K,F] : Run Spark locally with K worker threads and F maxFailures (see spark.task.maxFailures for an explanation of this variable)
 * - local[*] : Run Spark locally with as many worker threads as logical cores on your machine.
 * - local[*,F] : Run Spark locally with as many worker threads as logical cores on your machine and F maxFailures.
 *
 *
 */
object WordCount {

  def main(args: Array[String]): Unit = {

    val currentDirectory = new java.io.File(".").getCanonicalPath
    val dataDir = currentDirectory + "/data"
    val inputFile = dataDir + "/word_count.txt"

    val path = Paths.get(inputFile)
    println("fullpath:- " + path.toUri)
    val fileURI = path.toString;

    /** What is new SparkConf().setMaster("local[*]") */
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark Word Counter")

    // create Spark context with Spark configuration
    val sparkContext = new SparkContext(sparkConf.setAppName("SparkWordCount"))

    // connvert to RDD
    var linesRDD = sparkContext.textFile(fileURI)


    // flter RDD, ignoring first line
    val filterRDD = linesRDD.mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }

    var wordsRDD = filterRDD.flatMap(_.split(" "))
    var wordsKvRdd = wordsRDD.map((_, 1))
    var wordCounts = wordsKvRdd.reduceByKey(_ + _)


    val outDir = "/Users/mukthara/Downloads/out"
    val outDirPath = Paths.get(outDir)

    new File(outDirPath.toFile).deleteRecursively()

    wordCounts.saveAsTextFile(outDir)
  }

}