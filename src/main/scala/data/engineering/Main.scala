package data.engineering

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object Main {
  def main(args: Array[String]): Unit = {
    // build spark sess
    val spark = SparkSession.builder()
      .master(master = "local[*]")
      .appName("topXConfiguration")
      .getOrCreate()
    val conf = new SparkConf()
      .setAppName("topXConfiguration")
      .setMaster("local[*]") // Adjust as needed
    val sc = new SparkContext(conf)

    // Needs 4 inputs
    if (args.length != 4) {
      println("Usage: Main <inputPath1> <inputPath2> <outputPath> <top X display>")
      System.exit(1)
    }

    val inputPath1 = args(0) // Path to the first Parquet file
    val inputPath2 = args(1) // Path to the second Parquet file
    val outputPath = args(2) // Path to the output Parquet file
    val topX = try {
      args(3).toInt // Number of top items to consider
    } catch {
      case e: NumberFormatException =>
        println("The value for <top X display> must be an integer.")
        System.exit(1) // Exit with a non-zero status code
        0 // Return a default value (this line will not be reached if System.exit(1) is executed)
    }

    val numPartitions = 5 // Number of partitions is 5 by default

    // Parquet to RDD
    val rdd1 = spark.read.parquet(inputPath1).rdd
    val rdd2 = spark.read.parquet(inputPath2).rdd

    val itemCountRDD = Main.itemCount(rdd1) // Provide duplicate counts
    val uniqueRDD1 = removeDuplicates(rdd1) // Remove duplicates
    val topXResult = topXConfiguration(uniqueRDD1, rdd2, topX: Int, spark, outputPath) // Removes duplicates

    spark.stop()
  }

  // Function to remove duplicates and join
  def removeDuplicates(rdd1: RDD[Row]): RDD[Row] = {
    rdd1
      .map { row => (row.getAs[Long]("geographical_location_oid"), row.getAs[String]("item_name")) } // Map to (geographical_location_oid, item_name)
      .distinct() // Remove duplicates based on ("geographical_location_oid", item_name)
      .map { case (geoOid, itemName) => Row(geoOid, itemName) }
  }


  // Function to aggregate and remove duplicates
  def itemCount(rdd1: RDD[Row]): RDD[(Long, Long, Long, Long, Long, Int)] = {
    rdd1
      .map { row =>
        val geoOid = row.getAs[Long]("geographical_location_oid")
        val videoOid = row.getAs[Long]("video_camera_oid")
        val detectionOid = row.getAs[Long]("detection_oid")
        val itemName = row.getAs[Long]("item_name")
        val timestamp = row.getAs[Long]("timestamp_detected")
        ((geoOid, videoOid, detectionOid, itemName, timestamp), 1)
      } // Map each row to a tuple
      .reduceByKey(_ + _) // Aggregate counts by key
      .map { case ((geoOid, videoOid, detectionOid, itemName, timestamp), itemCount) =>
        (geoOid, videoOid, detectionOid, itemName, timestamp, itemCount)
      } // Map back to the desired format
  }


  // Function to find top X configuration
  def topXConfiguration(uniqueRDD1: RDD[Row], rdd2:RDD[Row], topX: Int, spark: SparkSession, outputPath: String): Unit ={
    val uniqueRDD1Map = uniqueRDD1
      .map{row =>
        val geoOid = row.getAs[Long]("geographical_location_oid")
        val itemName = row.getAs[String]("item_name")
        (geoOid, itemName)
      }

    val rdd2Map = rdd2
      .map{row =>
        val geoOid = row.getAs[Long]("geographical_location_oid")
        val geoLocation = row.getAs[String]("geographical_location")
        (geoOid, geoLocation)
      }

    // Use cogroup to join the two RDDs
    val cogroupedRDD = uniqueRDD1Map.cogroup(rdd2Map)

    // Process cogrouped to desired format
    val geoItemCounts = cogroupedRDD.flatMap { case (geoOid, (itemNames, geoLocations)) =>
      for {
        itemName <- itemNames
        geoLocation <- geoLocations
      } yield ((geoOid, itemName, geoLocation), 1) // to represent ("geographical_location_oid", "item_name", "geographical_location")
    }.reduceByKey(_ + _)

    // Transform to ((geoOid, geoLocation), (itemName, count))
    val geoItemCountPairs = geoItemCounts.map { case ((geoOid, itemName, geoLocation), count) =>
      ((geoOid, geoLocation), (itemName, count))
    }

    // Group by geoOid and sort by count and take TopX
    val rankedItems = geoItemCountPairs
      .groupByKey()
      .flatMap { case ((geoOid, geoLocation), items) =>
      val sortedItems = items.toList.sortBy(-_._2).take(topX)
        sortedItems.zipWithIndex.map { case ((itemName, count), rank) =>
        (geoLocation, rank + 1, itemName) // First rank is 1
      }
    }

    // Define the schema for the DataFrame with item_rank
    val schema = StructType(
      Seq(
        StructField("geographical_location", StringType, true),
        StructField("item_rank", IntegerType, true),
        StructField("item_name", StringType, true)
      )
    )

    // Convert the rankedItems RDD to a DataFrame in desired format
    val rankedItemsDF = spark.createDataFrame(rankedItems.map {
      case (geographical_location, item_rank, item_name) => Row(geographical_location, item_rank, item_name)
    }, schema)

    // Write the DataFrame to a Parquet file
    rankedItemsDF.write.parquet(outputPath)

  }

}

