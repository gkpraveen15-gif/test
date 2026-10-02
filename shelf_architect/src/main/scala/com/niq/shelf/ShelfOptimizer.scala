package com.niq.shelf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ShelfOptimizer {
  def main(args: Array[String]): Unit = {
    // 1. Initialize Spark Session (The engine for Big Data at NIQ)
    val spark = SparkSession.builder()
      .appName("Shelf Architect Data Pipeline")
      .master("local[*]") // Run locally for dev
      .getOrCreate()

    import spark.implicits._

    println("🚀 Starting Shelf Architect Pipeline...")

    // 2. Mocking Data: Massive Retail POS (Point of Sale) Data
    val posData = Seq(
      ("Store_001", "UPC_111", 500, "2023-10-01"),
      ("Store_001", "UPC_222", 150, "2023-10-01"),
      ("Store_002", "UPC_111", 300, "2023-10-01")
    ).toDF("store_id", "product_upc", "sales_volume", "date")

    // 3. Mocking Data: Planogram (How shelves are supposed to be arranged)
    val planogramData = Seq(
      ("Store_001", "UPC_111", "Aisle_4", 2),
      ("Store_001", "UPC_222", "Aisle_4", 1),
      ("Store_002", "UPC_111", "Aisle_2", 3)
    ).toDF("store_id", "product_upc", "aisle", "facings")

    // 4. The Transformation: Join POS and Planogram to find Shelf ROI
    val shelfInsights = posData
      .join(planogramData, Seq("store_id", "product_upc"), "inner")
      .withColumn("sales_per_facing", $"sales_volume" / $"facings")
      .orderBy(desc("sales_per_facing"))

    println("📊 Generated Shelf Optimization Insights:")
    shelfInsights.show()

    println("✅ Pipeline Execution Complete. Shutting down Spark.")
    spark.stop()
  }
}
