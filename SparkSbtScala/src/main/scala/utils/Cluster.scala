package org.muks.example
package utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

class Cluster {
  var sConf: SparkConf = null
  var sContext: SparkContext = null
  var sSession: SparkSession = null


  def setSparkConf(masterUrl: String, appName: String): Cluster = {
    this.sConf = new SparkConf().setMaster(masterUrl).setAppName(appName)
    this.sConf.set("spark.task.maxDirectResultSize", "10g")
    this.sConf.set("spark.sql.autoBroadcastJoinThreshold", "10g")
    this
  }

  def setSparkContext(): Cluster = {
    // create Spark context with Spark configuration
    this.sContext = new SparkContext(this.sConf)
    this
  }

  def getSparkSession() : SparkSession = {
    return SparkSession.builder.config(sContext.getConf).getOrCreate()
  }

}
