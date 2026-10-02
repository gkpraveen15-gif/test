package com.niq.shelf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object ShelfOptimizer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NIQ Enterprise Shelf Architect")
      .master("local[*]")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    println("==========================================================")
    println("🚀 INITIALIZING NIQ SHELF ARCHITECT ENTERPRISE PIPELINE")
    println("==========================================================")

    // ==========================================
    // 1. DATA MODELING: DIMENSIONS (Master Data)
    // ==========================================
    println("\n[1/5] Loading Dimensional Data (dim_product, dim_store)...")
    val dimProduct = Seq(
      ("UPC_111", "Brand_A", "Beverage", 12.0, true),
      ("UPC_222", "Brand_B", "Beverage", 12.0, false),
      ("UPC_333", "Brand_A", "Snacks", 8.0, true)
    ).toDF("upc_key", "brand_name", "category", "size_oz", "is_focus_brand")

    val dimStore = Seq(
      ("STR_001", "MKT_EAST", "Walmart", "Supercenter"),
      ("STR_002", "MKT_EAST", "Target", "Local"),
      ("STR_003", "MKT_WEST", "Walmart", "Supercenter")
    ).toDF("store_key", "market_id", "retailer_name", "store_format")

    // ==========================================
    // 2. DATA MODELING: FACTS (Transactional Data)
    // ==========================================
    println("[2/5] Loading Fact Data (fact_sales, fact_assortment)...")
    val factSales = Seq(
      // store_key, upc_key, week_id, sales_units, sales_dollars
      ("STR_001", "UPC_111", 202301, 500, 1000.0),
      ("STR_001", "UPC_222", 202301, 150, 200.0),
      ("STR_002", "UPC_111", 202301, -10, -20.0), // Intentional bad data for QC
      ("STR_003", "UPC_999", 202301, 50, 100.0)   // Intentional orphan data (UPC_999) for QC
    ).toDF("store_key", "upc_key", "week_id", "sales_units", "sales_dollars")

    val factAssortment = Seq(
      // store_key, upc_key, target_facings, status
      ("STR_001", "UPC_111", 3, "ACTIVE"),
      ("STR_001", "UPC_222", 1, "ACTIVE"),
      ("STR_002", "UPC_111", 2, "ACTIVE"),
      ("STR_003", "UPC_111", 4, "DELIST_PENDING")
    ).toDF("store_key", "upc_key", "target_facings", "planogram_status")

    // ==========================================
    // 3. QUALITY CONTROL (QC) MODULE
    // ==========================================
    println("\n[3/5] EXECUTING DATA QUALITY CONTROL (QC) GATE...")
    
    // Rule 1: Integrity Check (No negative sales allowed)
    val negativeSales = factSales.filter($"sales_units" < 0)
    if (negativeSales.count() > 0) {
      println("⚠️ QC WARNING: Found negative sales_units. Cleaning data...")
      negativeSales.show()
    }
    val cleanFactSales = factSales.filter($"sales_units" >= 0)

    // Rule 2: Referential Integrity (Orphan check: Sales without a valid Product Dimension)
    val orphanRecords = cleanFactSales.join(dimProduct, Seq("upc_key"), "left_anti")
    if (orphanRecords.count() > 0) {
      println("⚠️ QC WARNING: Orphan records detected in fact_sales (Unknown UPC). Quarantining...")
      orphanRecords.show()
    }
    
    // Final Validated Facts joined with Dims
    val validFactSales = cleanFactSales
      .join(dimProduct, Seq("upc_key"), "inner")
      .join(dimStore, Seq("store_key"), "inner")

    println("✅ QC Passed: Data cleansed and referential integrity verified.")

    // ==========================================
    // 4. BUSINESS LOGIC: STORE LEVEL ASSORTMENT
    // ==========================================
    println("\n[4/5] Processing STORE LEVEL Data Mart (Assortment Compliance & Space ROI)...")
    val storeLevelDF = validFactSales
      .join(factAssortment, Seq("store_key", "upc_key"), "left_outer")
      .withColumn("target_facings", coalesce($"target_facings", lit(0))) // Fill nulls
      .withColumn("sales_per_facing", 
        when($"target_facings" > 0, round($"sales_units" / $"target_facings", 2)).otherwise(0.0)
      )
      .withColumn("assortment_compliance",
        when($"target_facings" > 0 && $"sales_units" > 0, "COMPLIANT")
        .when($"target_facings" > 0 && $"sales_units" === 0, "ZERO_SCAN_ISSUE")
        .when($"target_facings" === 0 && $"sales_units" > 0, "UNAUTHORIZED_ITEM")
        .otherwise("UNKNOWN")
      )

    println("📊 STORE LEVEL FACTS:")
    storeLevelDF.select(
      "store_key", "upc_key", "retailer_name", 
      "sales_units", "target_facings", "sales_per_facing", "assortment_compliance"
    ).show()

    // ==========================================
    // 5. BUSINESS LOGIC: MARKET LEVEL SUMMARIES
    // ==========================================
    println("\n[5/5] Processing MARKET LEVEL Data Mart (Strategic Insights)...")
    val marketLevelDF = storeLevelDF
      .groupBy("market_id", "upc_key", "brand_name", "category", "is_focus_brand")
      .agg(
        sum("sales_dollars").alias("market_sales_$"),
        round(avg("sales_per_facing"), 2).alias("avg_sales_per_facing"),
        sum(when($"assortment_compliance" === "COMPLIANT", 1).otherwise(0)).alias("compliant_stores"),
        countDistinct("store_key").alias("total_stores")
      )
      // Distribution Depth is what percentage of stores in a market actually stock and sell the item correctly
      .withColumn("distribution_depth_pct", round(($"compliant_stores" / $"total_stores") * 100, 2))

    println("📊 MARKET LEVEL FACTS:")
    marketLevelDF.select(
      "market_id", "brand_name", "upc_key", 
      "market_sales_$", "avg_sales_per_facing", "distribution_depth_pct"
    ).orderBy("market_id", desc("market_sales_$")).show()

    println("\n==========================================================")
    println("✅ PIPELINE COMPLETE. READY FOR DATA WAREHOUSE INGESTION.")
    println("==========================================================")

    spark.stop()
  }
}
