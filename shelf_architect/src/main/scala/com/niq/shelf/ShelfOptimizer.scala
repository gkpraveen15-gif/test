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

    println("\n====================================================================================================")
    println(s"🌟 EXECUTIVE SUMMARY: SHELF ARCHITECT MARKET STUDY FOR $CLIENT_MANUFACTURER")
    println("====================================================================================================")
    println("▶ THE BUSINESS PROBLEM:")
    println(s"  $CLIENT_MANUFACTURER needs to know which products are winning on the shelf and which are wasting space.")
    println("  This application analyzes Point-of-Sale (POS) data across multiple markets to recommend exactly")
    println("  which products to keep (CORE_PROTECT) and which to drop (DELIST_CANDIDATE) based on velocity.")
    println("\n▶ PRIVACY COMPLIANCE:")
    println("  Per retailer syndication agreements, all Competitor Private Label data will be securely masked")
    println("  and aggregated before final delivery to the client.")
    println("====================================================================================================\n")

    // ==========================================
    // 1. ADVANCED DIMENSIONS
    // ==========================================
    println("▶ [PHASE 1] INGESTING MASTER DATA (PRODUCTS & STORES)...")
    println("  - Mapping the product hierarchy (Department > Category > Sub-Category).")
    println("  - Mapping Stores to their respective Markets (e.g., MKT_EAST, MKT_WEST).")
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
    println("\n▶ [PHASE 2] INGESTING TRANSACTIONAL SALES DATA...")
    println(s"  - Processing weekly register scans for $CLIENT_MANUFACTURER and competitors.")
    val factSales = Seq(
      // store_id, product_id, period_id, sales_units, sales_dollars
      ("STR_001", "UPC_101", "2023_W01", 100, 200.0),
      ("STR_001", "UPC_102", "2023_W01", 20, 40.0),
      ("STR_001", "UPC_103", "2023_W01", 150, 150.0), // High volume private label
      ("STR_001", "UPC_201", "2023_W01", 5, 15.0),    // Slow mover
      ("STR_002", "UPC_101", "2023_W01", 90, 180.0)
    ).toDF("store_id", "product_id", "period_id", "sales_units", "sales_dollars")

    val masterDF = factSales
      .join(dimProduct, "product_id")
      .join(dimStore, "store_id")

    // ==========================================
    // 3. BUSINESS LOGIC: SLOW MOVER DETECTION
    // ==========================================
    println("\n▶ [PHASE 3] EXECUTING ASSORTMENT INTELLIGENCE ALGORITHMS...")
    println("  - Story: Not all products sell at the same speed. We are now ranking every product's velocity")
    println("    (units sold) against other products in the *exact same Market, Period, and Sub-Category*.")
    println("  - Insight: Products in the top 20% are flagged as CORE_PROTECT (Do not touch these!).")
    println("    Products in the bottom 20% are flagged as DELIST_CANDIDATE (Remove these to free up shelf space).")
    
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
    println("\n▶ [PHASE 4] SECURING DATA FOR SYNDICATION DELIVERY...")
    println("  - Story: Retailers demand strict confidentiality regarding their 'Store Brands' (Private Labels).")
    println(s"  - Action: Scanning data for Private Label brands. Obfuscating UPCs and Characteristics so $CLIENT_MANUFACTURER")
    println("    can see the total market volume, but cannot reverse-engineer the exact retailer strategies.")
    
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

    println("\n▶ [PHASE 5] GENERATING FINAL DATA MART...")
    println("  - Setting final delivery granularity strictly to: [Market -> Store -> Product -> Period].")
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

    println("\n====================================================================================================")
    println(s"📊 FINAL CLIENT DATA MART (READY FOR $CLIENT_MANUFACTURER TO INGEST)")
    println("====================================================================================================")
    finalClientDeliveryMart.select(
      "market_id", "store_id", "period_id", "product_id_masked", 
      "brand_name_masked", "total_dollars", "assortment_action"
    ).show(truncate = false)

    println("\n====================================================================================================")
    println("✅ PIPELINE COMPLETE. THE DATA STORY HAS BEEN SUCCESSFULLY GENERATED AND SECURED.")
    println("====================================================================================================\n")

    spark.stop()
  }
}
