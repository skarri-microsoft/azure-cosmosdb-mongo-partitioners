package com.microsoft.azure.cosmos.mongodb.partitioner

import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.rdd.partitioner.{BsonValueOrdering, MongoPartition, MongoPartitioner, PartitionerHelper}
import org.bson.{BsonDocument, BsonString, BsonValue}

class MongoStringNumberPartitioner extends MongoPartitioner {

  private implicit object BsonValueOrdering extends BsonValueOrdering

  private var MatchQuery = new BsonDocument()

  /**
    * Creates the upper and lower boundary query for the given key
    *
    * @param key   the key that represents the values that can be partitioned
    * @param lower the value of the lower bound
    * @param upper the value of the upper bound
    * @return the document containing the partition bounds
    */
  def createEqQuery(key: String, number: BsonValue): BsonDocument = {
    require(Option(number).isDefined, "number partition key missing")
    val query = new BsonDocument()
    query.append("$eq", number)
    new BsonDocument(key, query)
  }

  def createNumberPartitions(
                              partitionKey: String,
                              lowest:    Int,
                              highest:    Int,
                              locations:    Seq[String] = Nil
                            ): Array[MongoPartition] = {
    (lowest to highest).toList.zipWithIndex.map({
      case (n: Int, i: Int) => MongoPartition(
        i,
        createEqQuery(
          partitionKey,
          new BsonString(n.toString)
        ),
        locations
      )
    }).toArray
  }
  /**
    * The partition key property
    */
  val partitionKeyProperty = "partitionKey".toLowerCase()
  val lowestNumberProperty = "lowestNumber".toLowerCase()
  val highestNumberProperty = "highestNumber".toLowerCase()

  /**
    * Calculate the Partitions
    *
    * @param connector  the MongoConnector
    * @param readConfig the [[com.mongodb.spark.config.ReadConfig]]
    * @return the partitions
    */
  override def partitions(
                           connector:  MongoConnector,
                           readConfig: ReadConfig,
                           pipeline:   Array[BsonDocument]
                         ): Array[MongoPartition] = {
    MatchQuery = PartitionerHelper.matchQuery(pipeline)
    System.out.println("Received pipeline: " + MatchQuery.toJson)
    val partitionerOptions = readConfig.partitionerOptions.map(kv => (kv._1.toLowerCase, kv._2))
    val shardKey = partitionerOptions(partitionKeyProperty)
    val lowestNumber = partitionerOptions(lowestNumberProperty).toInt
    val highestNumber =partitionerOptions(highestNumberProperty).toInt
    val partitions = createNumberPartitions(shardKey, lowestNumber,highestNumber)
    partitions
  }
}

case object MongoStringNumberPartitioner extends MongoStringNumberPartitioner