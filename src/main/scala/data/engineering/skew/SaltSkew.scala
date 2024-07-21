package data.engineering.skew

import org.apache.spark

import scala.util.Random.nextInt
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.util.Random

object SaltSkew {

  object Main {
    def main(args: Array[String]): Unit = {
      if (args.length != 4) {
        println("Usage: program <inputPath1> <inputPath2> <outputPath> <top X display>")
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
          System.exit(1)
          0 // This is a placeholder, System.exit will terminate the program before this line
      }

      val spark = SparkSession.builder()
        .appName("TopXConfiguration")
        .master("local[*]") // Adjust based on your cluster configuration
        .getOrCreate()

      // Read the input Parquet files
      val uniqueRDD1 = spark.read.parquet(inputPath1).rdd
      val rdd2 = spark.read.parquet(inputPath2).rdd

      // Remove duplicates from uniqueRDD1
      val deduplicatedUniqueRDD1 = removeDuplicates(uniqueRDD1)

      // Salt the uniqueRDD1
      val saltedUniqueRDD1 = saltRDD(deduplicatedUniqueRDD1, saltFactor = 10)

      // Call the function to transform the RDD and save as a Parquet file
      topXConfiguration(saltedUniqueRDD1, rdd2, topX, spark, outputPath)

      // Stop the SparkSession when done
      spark.stop()
    }

    // Function to remove duplicates and join
    def removeDuplicates(rdd1: RDD[Row]): RDD[Row] = {
      rdd1
        .map { row => (row.getAs[Long]("geographical_location_oid"), row.getAs[String]("item_name")) } // Map to (geographical_location_oid, item_name)
        .distinct() // Remove duplicates based on ("geographical_location_oid", item_name)
        .map { case (geoOid, itemName) => Row(geoOid, itemName) }
    }

    // Function to salt an RDD
    def saltRDD(rdd: RDD[Row], saltFactor: Int): RDD[Row] = {
      rdd.flatMap(row => {
        val geoOid = row.getAs[Long]("geographical_location_oid")
        val itemName = row.getAs[String]("item_name")
        (0 until saltFactor).map(salt => {
          val salt = Random.nextInt(saltFactor)
          Row.fromSeq(Seq(geoOid, itemName, salt)) // Create a new Row with the salt value appended
        })
      })
    }

    // Function to transform the RDD and save it as a Parquet file
    def topXConfiguration(saltedUniqueRDD1: RDD[Row], rdd2: RDD[Row], topX: Int, spark: SparkSession, outputPath: String): Unit = {

      // Map the rows of saltedUniqueRDD1 to ((geoOid, salt), item_name)
      val uniqueRDD1Map = saltedUniqueRDD1.map(row =>
        ((row.getAs[Long]("geographical_location_oid"), row.getAs[Int]("salt")), row.getAs[String]("item_name"))
      )

      // Map the rows of rdd2 to ((geoOid, salt), geographical_location) - No salting in rdd2
      val rdd2Map = rdd2.map(row =>
        ((row.getAs[Long]("geographical_location_oid"), 0), row.getAs[String]("geographical_location"))
      )

      // Use cogroup to join the two RDDs
      val cogroupedRDD = uniqueRDD1Map.cogroup(rdd2Map)

      // Process the cogrouped data to produce the desired output
      val geoItemCounts = cogroupedRDD.flatMap { case ((geoOid, salt), (itemNames, geoLocations)) =>
        for {
          itemName <- itemNames
          geoLocation <- geoLocations
        } yield ((geoOid, itemName, geoLocation), 1)
      }.reduceByKey(_ + _)

      // Transform to ((geoOid, geoLocation), (itemName, count))
      val geoItemCountPairs = geoItemCounts.map { case ((geoOid, itemName, geoLocation), count) =>
        ((geoOid, geoLocation), (itemName, count))
      }

      // Group by geoOid and sort by count and take TopX
      val rankedItems = geoItemCountPairs.groupByKey().flatMap { case ((geoOid, geoLocation), items) =>
        items.toList.sortBy(-_._2).take(topX).zipWithIndex.map { case ((itemName, count), rank) =>
          (geoLocation, rank + 1, itemName) // First rank is 1
        }
      }

      // Define the schema for the DataFrame
      val schema = StructType(Seq(
        StructField("geographical_location", StringType, nullable = false),
        StructField("rank", IntegerType, nullable = false),
        StructField("item_name", StringType, nullable = false)
      ))

      // Convert the rankedItems RDD to a DataFrame
      val rankedItemsDF = spark.createDataFrame(rankedItems.map {
        case (geographical_location, item_rank, item_name) => Row(geographical_location, item_rank, item_name)
      }, schema)

      // Write the DataFrame to a Parquet file
      rankedItemsDF.write.parquet(outputPath)
    }
  }
}
