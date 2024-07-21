package data.engineering

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.rdd.RDD


class MainTest extends AnyFunSuite with BeforeAndAfter {
  test("testRemoveDuplicates") {
    val spark = SparkSession.builder()
      .master("local[2]")  // 2 core for testing
      .appName("test")
      .getOrCreate()
    import spark.implicits._

    // Sample input and output paths
    val inputPath1 = "path/to/input1.parquet"
    val inputPath2 = "path/to/input2.parquet"
    val outputPath = "path/to/output.parquet"
    val topX = 3  // Example value for top X

    // Sample data RDD for testing removeDuplicates
    val rddDuplicate = spark.sparkContext.parallelize(Seq(
      Row(1L, "item1"),
      Row(2L, "item2"),
      Row(3L, "item3"),
      Row(4L, "item4"),
      Row(1L, "item1")  // Duplicate row
    ))
    // Sample data RDD for testing TopXConfiguration
    val rdd1 = spark.sparkContext.parallelize(Seq(
      Row(1L, 1L, 1L, "item1", 12345L),
      Row(1L, 1L, 2L, "item2", 12345L),
      Row(2L, 1L, 1L, "item1", 12345L),
      Row(1L, 1L, 1L, "item1", 12346L)
    ))
    val rdd2 = spark.sparkContext.parallelize(Seq(
      Row(1L, "A"),
      Row(1L, "B"),
      Row(2L, "A"),
      Row(1L, "A")
    ))

    val schema1 = StructType(Seq(
      StructField("geographical_location_oid", LongType, nullable = false),
      StructField("item_name", StringType, nullable = false)
    ))

    val expectedSchema = StructType(Seq(
        StructField("geographical_location", StringType, true),
        StructField("item_rank", IntegerType, true),
        StructField("item_name", StringType, true)
      ))

    val RemoveDuplicateDF = spark.createDataFrame(rddDuplicate, schema1).rdd
    val outputDF = Main.topXConfiguration(spark.read.parquet(inputPath1).rdd,
      spark.read.parquet(inputPath2).rdd, topX, spark, outputPath)

    // Test removeDuplicates function
    val uniqueRDD = Main.removeDuplicates(RemoveDuplicateDF)
    val uniqueCount = uniqueRDD.count()
    assert(uniqueCount == 4)  // Expected unique rows count


    // Read output
    val outputDF = spark.read.parquet(outputPath)

    // Test TopXConfiguration function
    val expectedRank = 1
    assert(new java.io.File(outputPath).exists()) // Check path exists
    assert(outputDF.count() > 0)  // Validate output data exists
    assert(outputDF.schema == expectedSchema)  // Validate schema
    assert(outputDF.filter($"item_name" === "item1")
      .select("item_rank").head().getInt(0) == expectedRank)

  }
  spark.stop()
}

