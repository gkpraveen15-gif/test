package com.niq.shelf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object ShelfOptimizer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NIQ Enterprise Shelf Architect - Client Facing")
      .master("local[*]")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val CLIENT_MANUFACTURER = "Brand_A"

    println("==========================================================")
    println(s"🚀 SHELF ARCHITECT PIPELINE (CLIENT: $CLIENT_MANUFACTURER)")
    println("==========================================================")

    // ==========================================
    // 1. ADVANCED DIMENSIONS
    // ==========================================
    println("\n[1/4] Building Dimensions (Product & Store)...")
    val dimProduct = Seq(
      ("UPC_101", "Grocery", "Beverage", "Energy", "Brand_A", "Premium", "Sugar-Free"),
      ("UPC_102", "Grocery", "Beverage", "Energy", "Brand_B", "Mainstream", "Regular"),
      ("UPC_103", "Grocery", "Beverage", "Energy", "PrivateLabel_Walmart", "Value", "Regular"),
      ("UPC_201", "Grocery", "Snacks", "Chips", "Brand_A", "Premium", "Organic")
    ).toDF("product_id", "department", "category", "sub_category", "brand_name", "segmentation", "characteristics")

    val dimStore = Seq(
      ("STR_001", "MKT_EAST"),
      ("STR_002", "MKT_WEST"),
      ("STR_003", "MKT_EAST")
    ).toDF("store_id", "market_id")

    // ==========================================
    // 2. RAW FACT DATA
    // ==========================================
    println("[2/4] Loading Transactional Fact Data...")
    val factSales = Seq(
      // store_id, product_id, period_id, sales_units, sales_dollars
      ("STR_001", "UPC_101", "2023_W01", 100, 200.0),
      ("STR_001", "UPC_102", "2023_W01", 20, 40.0),
      ("STR_001", "UPC_103", "2023_W01", 150, 150.0), // High volume private label
      ("STR_001", "UPC_201", "2023_W01", 5, 15.0),    // Slow mover
      ("STR_002", "UPC_101", "2023_W01", 90, 180.0)
    ).toDF("store_id", "product_id", "period_id", "sales_units", "sales_dollars")

    // Master join
    val masterDF = factSales
      .join(dimProduct, "product_id")
      .join(dimStore, "store_id")

    // ==========================================
    // 3. BUSINESS LOGIC: SLOW MOVER DETECTION
    // ==========================================
    println("\n[3/4] Executing Assortment Algorithms...")
    
    // Rank products within their Market, Period, and Sub-Category based on volume
    val categoryWindow = Window.partitionBy("market_id", "period_id", "sub_category").orderBy(desc("sales_units"))
    
    val assortmentScoringDF = masterDF
      .withColumn("velocity_rank_pct", percent_rank().over(categoryWindow))
      .withColumn("assortment_action",
        when($"velocity_rank_pct" >= 0.80, "DELIST_CANDIDATE")
        .when($"velocity_rank_pct" <= 0.20, "CORE_PROTECT")
        .otherwise("MAINTAIN")
      )

    // ==========================================
    // 4. DATA SECURITY: MASKING FOR CLIENT DELIVERY
    // ==========================================
    println(s"\n[4/4] Applying Data Restrictions for Client Delivery ($CLIENT_MANUFACTURER)...")
    
    val clientFacingDF = assortmentScoringDF
      .withColumn("brand_name_masked",
        when($"brand_name".like("PrivateLabel%"), lit("RESTRICTED_PRIVATE_LABEL"))
        .otherwise($"brand_name")
      )
      .withColumn("product_id_masked",
        when($"brand_name".like("PrivateLabel%"), lit("MASKED_UPC"))
        .otherwise($"product_id")
      )
      .withColumn("characteristics_masked",
        when($"brand_name".like("PrivateLabel%"), lit("RESTRICTED"))
        .otherwise($"characteristics")
      )

    // Aggregate to the required Granularity: Market, Store, Product, Period
    val finalClientDeliveryMart = clientFacingDF
      .groupBy(
        "market_id", "store_id", "product_id_masked", "period_id",
        "department", "category", "sub_category", 
        "brand_name_masked", "segmentation", "characteristics_masked", "assortment_action"
      )
      .agg(
        sum("sales_units").alias("total_units"),
        sum("sales_dollars").alias("total_dollars")
      )
      .orderBy(col("market_id"), col("store_id"), col("period_id"), desc("total_dollars"))

    println(s"🔒 SECURE CLIENT DATA MART (Granularity: Market -> Store -> Product -> Period):")
    finalClientDeliveryMart.select(
      "market_id", "store_id", "product_id_masked", "period_id",
      "brand_name_masked", "total_dollars", "assortment_action"
    ).show(truncate = false)

    println("\n==========================================================")
    println("✅ PIPELINE COMPLETE. READY FOR CLIENT INGESTION.")
    println("==========================================================")

    spark.stop()
  }
}
