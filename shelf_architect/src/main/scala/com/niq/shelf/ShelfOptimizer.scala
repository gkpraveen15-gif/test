package com.niq.shelf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object ShelfOptimizer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Advanced Retail Analytics")
      .master("local[*]")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    println("🚀 Starting Advanced Retail Pipeline (NIQ Style)...")

    // 1. INPUT: Product Characteristics (Focus vs Competitor Brands)
    val products = Seq(
      ("UPC_1", "OurBrand", "Beverage", true),   // Focus Brand
      ("UPC_2", "CompBrand", "Beverage", false), // Competitor
      ("UPC_3", "OurBrand", "Snack", true)       // Focus Brand
    ).toDF("upc", "brand", "category", "is_focus")

    // 2. INPUT: Store/Market Characteristics
    val stores = Seq(
      ("Store_A", "Focus_Market"),
      ("Store_B", "Competitive_Market"),
      ("Store_C", "Focus_Market")
    ).toDF("store_id", "market_type")

    // 3. INPUT: Weekly Sales Value & Units
    val sales = Seq(
      ("Store_A", "UPC_1", 1, 50, 100.0),
      ("Store_A", "UPC_1", 2, 55, 110.0),
      ("Store_A", "UPC_2", 1, 20, 40.0),  // Store A sells both
      ("Store_B", "UPC_2", 1, 200, 400.0),// Store B (Comp Market) loves UPC_2
      ("Store_C", "UPC_1", 1, 10, 20.0)   // Store C sells UPC_1 very poorly
      // Note: Store C does NOT sell UPC_3, and Store B does NOT sell UPC_1
    ).toDF("store_id", "upc", "week", "units", "revenue")

    // ==========================================
    // ALGORITHM 1: ROS (Rate of Sale / Velocity)
    // ==========================================
    // Average units sold per store, per week
    val rosDF = sales.groupBy("store_id", "upc")
      .agg(
        (sum("units") / countDistinct("week")).alias("ros_units"),
        sum("revenue").alias("total_revenue")
      )

    val masterDF = rosDF
      .join(products, "upc")
      .join(stores, "store_id")

    // ==========================================
    // ALGORITHM 2: Incrementality & Category Share
    // ==========================================
    // Does our focus brand actually grow the category, or just cannibalize?
    val categoryWindow = Window.partitionBy("store_id", "category")
    
    val categoryShareDF = masterDF
      .withColumn("store_category_revenue", sum("total_revenue").over(categoryWindow))
      .withColumn("brand_share_pct", round($"total_revenue" / $"store_category_revenue" * 100, 2))

    // ==========================================
    // ALGORITHM 3: Opportunity Finder (Opp Finder)
    // ==========================================
    // Find Focus Markets where our Focus Brands are NOT being sold (Distribution Voids)
    val allPossibleCombos = stores.filter($"market_type" === "Focus_Market")
      .crossJoin(products.filter($"is_focus" === true))
    
    val oppFinderDF = allPossibleCombos
      .join(rosDF, Seq("store_id", "upc"), "left_anti") // Find combos that DO NOT exist in sales
      .withColumn("opportunity", lit("MISSING_DISTRIBUTION"))

    println("\n🎯 OPPORTUNITY FINDER (Focus Markets missing our Focus Products):")
    oppFinderDF.select("store_id", "market_type", "upc", "brand", "opportunity").show()

    // ==========================================
    // ALGORITHM 4: Assortment Recommendation
    // ==========================================
    // KEEP, DROP, or ADD based on ROS and Market Type
    val marketAvgWindow = Window.partitionBy("market_type", "category")
    
    val assortmentDF = categoryShareDF
      .withColumn("market_avg_ros", avg("ros_units").over(marketAvgWindow))
      .withColumn("recommendation", 
        when($"is_focus" === true && $"ros_units" < $"market_avg_ros" * 0.5, "REVIEW/SUPPORT") // Our brand is dying here
        .when($"is_focus" === false && $"ros_units" < $"market_avg_ros", "DELIST")           // Competitor is weak, kick them out
        .otherwise("KEEP")
      )

    println("\n📦 ASSORTMENT & INCREMENTALITY RECOMMENDATIONS:")
    assortmentDF.select(
      "store_id", "market_type", "brand", "category", 
      "ros_units", "brand_share_pct", "recommendation"
    ).orderBy("store_id", "upc").show()

    spark.stop()
  }
}
