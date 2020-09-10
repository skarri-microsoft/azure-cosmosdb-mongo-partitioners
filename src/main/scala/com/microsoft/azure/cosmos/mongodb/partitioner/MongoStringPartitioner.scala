package com.microsoft.azure.cosmos.mongodb.partitioner

import java.util
import java.util.Collections

import com.mongodb.BasicDBObject
import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.rdd.partitioner.PartitionerHelper.createBoundaryQuery
import com.mongodb.spark.rdd.partitioner.{BsonValueOrdering, MongoPartition, MongoPartitioner, PartitionerHelper}
import org.bson.{BsonDocument, BsonString, Document}

class MongoStringPartitioner extends MongoPartitioner {

  private implicit object BsonValueOrdering extends BsonValueOrdering

  private val DefaultPartitionKey = "_id"
  private val DefaultNumberOfPartitions = "64"
  private val DefaultMinKey = "min"
  private val DefaultMaxKey = "max"
  private var MatchQuery = new BsonDocument()

  /**
    * The partition key property
    */
  val partitionKeyProperty = "partitionKey".toLowerCase()

  /**
    * The number of partitions property
    */
  val numberOfPartitionsProperty = "numberOfPartitions".toLowerCase()

  def FindCommonPrefixPosition(min: String, max: String): Int = {
    var indexPos = 0
    while (true) {
      if (min.length <= indexPos || max.length <= indexPos) {
        return indexPos - 1
      }
      if (min.charAt(indexPos) == max.charAt(indexPos))
        indexPos += 1
      else
        return indexPos
    }
    indexPos
  }

  def StringRanges(min: String, max: String): util.HashMap[String, String] = {
    val map: util.HashMap[String, String] = new util.HashMap[String, String]
    if (min.equals(max)) {
      map.put(min, max)
      return map
    }
    val prefixPos: Int = FindCommonPrefixPosition(min, max)

    var startingPrefix: Int = min.charAt(prefixPos) + 1
    val endingPrefix: Int = max.charAt(prefixPos) + 1
    val commonPrefix: String = min.substring(0, prefixPos)
    var minPrefix: String = min
    while (startingPrefix <= endingPrefix) {
      val maxPrefix = commonPrefix + startingPrefix.toChar
      if (maxPrefix.compareTo(max) > 0) {
        map.put(minPrefix, max)
        return map
      } else {
        map.put(minPrefix, maxPrefix)
        minPrefix = maxPrefix
        startingPrefix += 1
      }
      //Optimization
      // If you reach the max then don't add it, just return the processed ones
      if (minPrefix.equals(max)) return map
    }
    map
  }

  private def GetStringRanges(key: String, partitionSize: Long, connector: MongoConnector,
                              readConfig: ReadConfig, totalRecords: Long) = {
    val FinalRangeStats = new util.HashMap[String, MinMax]
    if (totalRecords <= 0) {
      System.out.println("Zero documents for the given match, existing string partitioner.")
      FinalRangeStats
    } else {
      val queue = new util.LinkedList[MinMax]
      queue.add(getMinMax(null, key, connector, readConfig))
      while (!queue.isEmpty) {
        val element = queue.poll
        val ranges = StringRanges(element.Min, element.Max)

        //Optimization, can't split any more irrespective of the partition size
        if (ranges.size == 1)
          FinalRangeStats.put(element.Min, element)
        else {
          val statRanges = getRangeStats(ranges, key, connector, readConfig)
          val it = statRanges.entrySet.iterator

          while (it.hasNext) {
            val pair = it.next.asInstanceOf[util.Map.Entry[_, _]]
            if (pair.getValue.asInstanceOf[MinMax].Count > partitionSize) {
              val filter = pair.getValue.asInstanceOf[MinMax]
              val check = getMinMax(filter, key, connector, readConfig)
              if (check.Min == check.Max) FinalRangeStats.put(pair.getKey.toString, pair.getValue.asInstanceOf[MinMax])
              else queue.add(getMinMax(filter, key, connector, readConfig))
            } else FinalRangeStats.put(pair.getKey.toString, pair.getValue.asInstanceOf[MinMax])
          }
        }
      }
      val RearrangedFinalStats = ReArrangeBoundries(FinalRangeStats)
      val updated = UpdateFinalRangeStats(RearrangedFinalStats, key, connector, readConfig)

      System.out.println("Printing final calculated ranges.....keys count: " + updated.size)
      //PrintFinalRangeStats(FinalRangeStats)
      PrintFinalRangeStats(updated)

      System.out.println("Sorting and Merging final calculated ranges.....")
      val sortAndMergedRanges = SortAndMergeFinalRangeStats(GetHashMap(updated), partitionSize)
      System.out.println("Validating Sorted and Merged ranges.....keys Count: " + sortAndMergedRanges.size)
      ValidateFinalSortedRange(sortAndMergedRanges, key, connector, readConfig)
      sortAndMergedRanges
    }
  }

  private def PrintFinalRangeStats(statRanges: util.TreeMap[String, MinMax]): Unit = {
    val it = statRanges.entrySet.iterator
    var totalCount = 0L
    while ({
      it.hasNext
    }) {
      val pair = it.next.asInstanceOf[util.Map.Entry[_, _]]
      val item = pair.getValue.asInstanceOf[MinMax]
      totalCount = totalCount + item.Count
      System.out.println("Min :" + item.Min + " Max: " + item.Max + " Count: " + item.Count)
    }
    System.out.println("Total number of documents: " + totalCount)
  }

  private def getUpdatedMinMax(item: MinMax, key: String, connector: MongoConnector,
                               readConfig: ReadConfig) = {
    val criteria = new util.ArrayList[BasicDBObject]
    val minFilter = new BasicDBObject(key, new BasicDBObject("$gte", item.Min))
    val maxFilter = new BasicDBObject(key, new BasicDBObject("$lt", item.Max))

    // Applying pipe line
    criteria.add(BasicDBObject.parse(MatchQuery.toJson))
    criteria.add(minFilter)
    criteria.add(maxFilter)
    item.Count = connector.withCollectionDo(readConfig, { coll: MongoCollection[BsonDocument] =>
      coll.count(new BasicDBObject("$and", criteria))
    })
    item
  }

  private def UpdateFinalRangeStats(statRangesInput: util.HashMap[String, MinMax], shardKey: String, connector: MongoConnector,
                                    readConfig: ReadConfig) = {
    val statRanges = new util.TreeMap[String, MinMax](statRangesInput)
    val keys = statRanges.keySet.toArray(new Array[String](statRanges.size))
    scala.util.Sorting.quickSort(keys)
    var i = 0
    while ({
      i < keys.length - 1
    }) {
      statRanges.put(keys(i), getUpdatedMinMax(statRanges.get(keys(i)), shardKey, connector, readConfig))
      i += 1;
      i - 1

    }
    // Last element
    statRanges.put(keys(keys.length - 1), getUpdatedMinMax(
      statRanges.get(keys(keys.length - 1)),
      shardKey, connector, readConfig
    ))
    statRanges
  }

  private def ReArrangeBoundries(statRanges: util.HashMap[String, MinMax]) = {
    val rearranged = new util.HashMap[String, MinMax]
    val keys = new util.ArrayList[String](statRanges.keySet)
    Collections.sort(keys)
    var i = 0
    while ({
      i < keys.size - 1
    }) {
      val current = statRanges.get(keys.get(i))
      val next = statRanges.get(keys.get(i + 1))
      rearranged.put(current.Min, current)
      next.Min = current.Max
      rearranged.put(next.Min, next)

      {
        i += 1;
        i - 1
      }
    }
    // Last one raise the max Condition
    val last = statRanges.get(keys.get(keys.size - 1))
    last.Max = "" + (last.Max.charAt(0) + 1).toChar
    rearranged.put(last.Min, last)
    rearranged
  }

  private def getMinMax(filter: MinMax, key: String, connector: MongoConnector,
                        readConfig: ReadConfig) = {
    val target = new MinMax
    val minDoc = BsonDocument.parse("{" + key + ":1}")
    val maxDoc = BsonDocument.parse("{" + key + ":-1}")

    // Applying pipe line
    val criteria = new util.ArrayList[BasicDBObject]
    criteria.add(BasicDBObject.parse(MatchQuery.toJson))
    if (filter != null) {

      val minFilter = new BasicDBObject(key, new BasicDBObject("$gte", filter.Min))
      val maxFilter = new BasicDBObject(key, new BasicDBObject("$lt", filter.Max))
      criteria.add(minFilter)
      criteria.add(maxFilter)
    }

    val findFilter = new Document("$and", criteria)
    connector.withCollectionDo(readConfig, { coll: MongoCollection[BsonDocument] =>
      target.Min = coll.find(findFilter).sort(minDoc).limit(1).first.getString(key).getValue
      target.Max = coll.find(findFilter).sort(maxDoc).limit(1).first.getString(key).getValue
    })
    target
  }

  private def getRangeStats(
                             ranges: util.HashMap[String, String],
                             key:    String, connector: MongoConnector,
                             readConfig: ReadConfig
                           ) = {
    val rangeStats = new util.HashMap[String, MinMax]
    val it = ranges.entrySet.iterator
    var totalCount = 0L
    while ({
      it.hasNext
    }) {
      val minMax = new MinMax
      val pair = it.next.asInstanceOf[util.Map.Entry[_, _]]
      System.out.println(pair.getKey + " = " + pair.getValue)
      minMax.Min = pair.getKey.toString
      minMax.Max = pair.getValue.toString
      val criteria = new util.ArrayList[BasicDBObject]
      val minFilter = new BasicDBObject(key, new BasicDBObject("$gte", pair.getKey))
      val maxFilter = new BasicDBObject(key, new BasicDBObject("$lt", pair.getValue))
      criteria.add(minFilter)
      criteria.add(maxFilter)

      //Apply pipe line
      criteria.add(BasicDBObject.parse(MatchQuery.toJson()))

      val count = connector.withCollectionDo(readConfig, { coll: MongoCollection[BsonDocument] =>
        coll.count(new BasicDBObject("$and", criteria))
      })
      minMax.Count = count
      rangeStats.put(minMax.Min, minMax)
      totalCount = totalCount + count

    }
    rangeStats
  }

  private def GetHashMap(updated: util.TreeMap[String, MinMax]) = {
    val hashMap = new util.HashMap[String, MinMax]
    val it = updated.entrySet.iterator
    val totalCount = 0
    while ({
      it.hasNext
    }) {
      val pair = it.next.asInstanceOf[util.Map.Entry[_, _]]
      val minMax = pair.getValue.asInstanceOf[MinMax]
      hashMap.put(pair.getKey.toString, minMax)
    }
    hashMap
  }

  private def ValidateFinalSortedRange(
                                        rangeStatsInput: util.HashMap[String, MinMax],
                                        key:             String,
                                        connector:       MongoConnector,
                                        readConfig:      ReadConfig
                                      ): Unit = {
    val rangeStats = new util.TreeMap[String, MinMax]
    rangeStats.putAll(rangeStatsInput)
    val it = rangeStats.entrySet.iterator
    var totalCount = 0L
    while ({
      it.hasNext
    }) {
      val pair = it.next.asInstanceOf[util.Map.Entry[_, _]]
      val minMax = pair.getValue.asInstanceOf[MinMax]
      val criteria = new util.ArrayList[BasicDBObject]
      val minFilter = new BasicDBObject(key, new BasicDBObject("$gte", minMax.Min))
      val maxFilter = new BasicDBObject(key, new BasicDBObject("$lt", minMax.Max))
      criteria.add(minFilter)
      criteria.add(maxFilter)
      //Apply pipe line
      criteria.add(BasicDBObject.parse(MatchQuery.toJson()))

      val count = connector.withCollectionDo(readConfig, { coll: MongoCollection[BsonDocument] =>
        coll.count(new BasicDBObject("$and", criteria))
      })
      System.out.println("Min: " + minMax.Min + " Max: " + minMax.Max + " Expected: " + minMax.Count + " Actual:" + count + " isEqual: " + (minMax.Count == count))
      totalCount = totalCount + count
    }
    System.out.println("Total number of documents : " + totalCount)
  }

  private def SortAndMergeFinalRangeStats(finalStatRanges: util.HashMap[String, MinMax], partitionSize: Long): util.HashMap[String, MinMax] = {
    if (finalStatRanges.size < 2) return finalStatRanges
    val statRanges = ReArrangeBoundries(finalStatRanges)
    val mergesRangesByPartitionSize = new util.HashMap[String, MinMax]
    val keys = new util.ArrayList[String](statRanges.keySet)
    Collections.sort(keys)
    var count = 0L
    var merged: MinMax = null
    var isMerged = false
    var i = 0
    while ({
      i < keys.size
    }) {
      val current = statRanges.get(keys.get(i))
      if (count == 0) merged = current
      // If this crosses partition size don't include
      if (count + current.Count > partitionSize) if (count == 0) {
        mergesRangesByPartitionSize.put(current.Min, current)
        isMerged = true
      } else {
        merged.Max = statRanges.get(keys.get(i - 1)).Max
        merged.Count = count
        mergesRangesByPartitionSize.put(merged.Min, merged)
        isMerged = true
      }
      else count = count + current.Count
      if (isMerged && count != 0) {
        merged = current
        count = current.Count
        isMerged = false
      }

      {
        i += 1;
        i - 1
      }
    }
    // Last element
    if (!isMerged) {
      merged.Max = statRanges.get(keys.get(keys.size - 1)).Max
      merged.Count = count
      mergesRangesByPartitionSize.put(merged.Min, merged)
    }
    mergesRangesByPartitionSize
  }

  private def SortAndMergeFinalRangeStats2(
                                            statRanges:    util.HashMap[String, MinMax],
                                            partitionSize: Long
                                          ): util.HashMap[String, MinMax] = {
    if (statRanges.size < 2) return statRanges
    val mergesRangesByPartitionSize = new util.HashMap[String, MinMax]
    val keys = new util.ArrayList[String](statRanges.keySet)
    Collections.sort(keys)
    var count = 0L
    var merged = new MinMax
    var isMerged = false
    var i = 0
    while (i < keys.size) {
      val current = statRanges.get(keys.get(i))
      if (count == 0) merged = current
      // If this crosses partition size don't include
      if (count + current.Count > partitionSize) {
        if (count == 0) {
          mergesRangesByPartitionSize.put(current.Min, current)
          isMerged = true
        } else {
          merged.Max = statRanges.get(keys.get(i - 1)).Max
          merged.Count = count
          mergesRangesByPartitionSize.put(merged.Min, merged)
          isMerged = true
        }
      } else count = count + current.Count
      if (isMerged && count != 0) {
        merged = current
        count = current.Count
        isMerged = false
      }
      i += 1

    }
    // Last element
    if (!isMerged) {
      merged.Max = statRanges.get(keys.get(keys.size - 1)).Max
      merged.Count = count
      mergesRangesByPartitionSize.put(merged.Min, merged)
    }
    mergesRangesByPartitionSize
  }

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
    System.out.println("Received pipeline: " + MatchQuery.toJson);
    val partitionerOptions = readConfig.partitionerOptions.map(kv => (kv._1.toLowerCase, kv._2))
    val shardKey = partitionerOptions.getOrElse(partitionKeyProperty, DefaultPartitionKey)
    val approxDocsPerSecond = partitionerOptions.getOrElse("approxDocsPerSecond".toLowerCase(), "1000").toInt
    val readBatchSize = partitionerOptions.getOrElse("readBatchSize".toLowerCase(), "50").toInt
    val totalDocuments = connector.withCollectionDo(readConfig, { coll: MongoCollection[BsonDocument] =>
      System.out.println("Collection Namespace :" + coll.getNamespace)
      coll.count(MatchQuery)
    })
    val parallelTasks = approxDocsPerSecond / readBatchSize
    val partitionSize = totalDocuments / parallelTasks
    val partitionRangeIds = GetStringRanges(shardKey, partitionSize, connector, readConfig, totalDocuments)
    val partitionRangeIdsSeq = partitionRangeIds.values().toArray(new Array[MinMax](partitionRangeIds.size)).toSeq
    val partitions = preparePartitions(shardKey, partitionRangeIdsSeq)
    partitions
  }

  def preparePartitions(
                         partitionKey: String,
                         splitKeys:    Seq[MinMax],
                         locations:    Seq[String] = Nil
                       ): Array[MongoPartition] = {
    splitKeys.zipWithIndex.map({
      case (minmax: MinMax, i: Int) => MongoPartition(
        i,
        createBoundaryQuery(
          partitionKey,
          new BsonString(minmax.Min),
          new BsonString(minmax.Max)
        ),
        locations
      )
    }).toArray
  }
}

case object MongoStringPartitioner extends MongoStringPartitioner

class MinMax {
  var Min: String = null
  var Max: String = null
  var Count = 0L
}