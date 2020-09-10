package com.microsoft.azure

import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]) {

    val uri = "url"
    val databaseName="test"
    val collectionName = "al"
    val partitionerValue="com.microsoft.azure.cosmos.mongodb.partitioner.MongoStringPartitioner$"
    val sc = SparkSession.builder()
      .master("local")
      .appName("custom-partitioner-demo")
      .config("spark.mongodb.input.uri", uri)
      .config("spark.mongodb.output.uri", uri)
      .getOrCreate().sparkContext

    val readConfig= ReadConfig(
      Map("uri" -> uri,"Database"->databaseName,"Collection"->collectionName,"spark.mongodb.input.partitioner"->partitionerValue,"spark.mongodb.input.partitionerOptions.approxDocsPerSecond"->"2000","spark.mongodb.input.partitionerOptions.partitionKey"->"storeId","batchSize"->"100"))

    import com.mongodb.spark.MongoSpark

    val dataSet = MongoSpark.load(sc, readConfig)

    println(dataSet.count())
  }
}
