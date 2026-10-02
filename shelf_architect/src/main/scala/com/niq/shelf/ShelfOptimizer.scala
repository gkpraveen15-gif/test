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

    // Client Context for Data Restrictions
    val CLIENT_MANUFACTURER = "Brand_A"

    println("==========================================================")
    println(s"🚀 INITIALIZING SHELF ARCHITECT PIPELINE FOR CLIENT: $CLIENT_MANUFACTURER")
    println("==========================================================")

    // ==========================================
    // 1. ADVANCED PRODUCT HIERARCHY & CHARACTERISTICS
    // ==========================================
    println("\n[1/4] Building Product Hierarchy & Segmentation Dimension...")
    val dimProduct = Seq(
      // upc_key, department, category, sub_category, brand_name, segmentation, characteristics
      ("UPC_101", "Grocery", "Beverage", "Energy Drinks", "Brand_A", "Premium", "Sugar-Free"),
      ("UPC_102", "Grocery", "Beverage", "Energy Drinks", "Brand_B", "Mainstream", "Regular"),
      ("UPC_103", "Grocery", "Beverage", "Energy Drinks", "PrivateLabel_Walmart", "Value", "Regular"),
      ("UPC_201", "Grocery", "Snacks", "Chips", "Brand_A", "Premium", "Organic"),
      ("UPC_202", "Grocery", "Snacks", "Chips", "PrivateLabel_Target", "Value", "Non-GMO")
    ).toDF("upc_key", "department", "category", "sub_category", "brand_name", "segmentation", "characteristics")

    // ==========================================
    // 2. RAW FACT DATA
    // ==========================================
    println("[2/4] Loading Transactional Fact Data...")
    val factSales = Seq(
      ("STR_001", "UPC_101", 12, 1200, 2400.0), // High performer
      ("STR_001", "UPC_102", 12, 300, 600.0),   // Mid performer
      ("STR_001", "UPC_103", 12, 800, 800.0),   // Private label, high volume low price
      ("STR_001", "UPC_201", 12, 40, 120.0),    // Slow mover
      ("STR_001", "UPC_202", 12, 50, 100.0)     // Slow mover
    ).toDF("store_key", "upc_key", "weeks_on_shelf", "sales_units", "sales_dollars")

    // Master join
    val masterDF = factSales.join(dimProduct, "upc_key")

    // ==========================================
    // 3. BUSINESS LOGIC: SLOW MOVER DETECTION (VELOCITY)
    // ==========================================
    println("\n[3/4] Executing Velocity Algorithms (Detecting Slow Movers)...")
    
    // Calculate ROS (Rate of Sale)
    val velocityDF = masterDF.withColumn("ros_units_per_week", $"sales_units" / $"weeks_on_shelf")

    // Industry Standard: Rank products within their Sub-Category
    val categoryWindow = Window.partitionBy("sub_category").orderBy(desc("ros_units_per_week"))
    
    val assortmentScoringDF = velocityDF
      .withColumn("velocity_rank_pct", percent_rank().over(categoryWindow))
      .withColumn("assortment_action",
        when($"velocity_rank_pct" >= 0.80, "DELIST_CANDIDATE (Bottom 20%)")
        .when($"velocity_rank_pct" <= 0.20, "CORE_PROTECT (Top 20%)")
        .otherwise("MAINTAIN")
      )

    println("📊 VELOCITY & ASSORTMENT SCORING (Unmasked Internal View):")
    assortmentScoringDF.select(
      "sub_category", "brand_name", "upc_key", 
      "ros_units_per_week", "velocity_rank_pct", "assortment_action"
    ).orderBy("sub_category", "velocity_rank_pct").show(truncate = false)

    // ==========================================
    // 4. DATA SECURITY: MASKING & RESTRICTIONS FOR CLIENT DELIVERY
    // ==========================================
    println(s"\n[4/4] Applying Data Restrictions for Client Delivery ($CLIENT_MANUFACTURER)...")
    
    // Rule: Retailers do not allow Manufacturers to see Private Label UPCs or specific Names.
    // They must be aggregated/masked into a generic "RESTRICTED_PRIVATE_LABEL" bucket.
    val clientFacingDF = assortmentScoringDF
      .withColumn("brand_name_masked",
        when($"brand_name".like("PrivateLabel%"), lit("RESTRICTED_PRIVATE_LABEL"))
        .otherwise($"brand_name")
      )
      .withColumn("upc_key_masked",
        when($"brand_name".like("PrivateLabel%"), lit("MASKED_UPC"))
        .otherwise($"upc_key")
      )
      .withColumn("characteristics_masked",
        when($"brand_name".like("PrivateLabel%"), lit("RESTRICTED"))
        .otherwise($"characteristics")
      )

    // Aggregate the masked data so the client cannot reverse-engineer individual private label items
    val finalClientDeliveryMart = clientFacingDF
      .groupBy(
        "department", "category", "sub_category", 
        "brand_name_masked", "upc_key_masked", "segmentation", 
        "characteristics_masked", "assortment_action"
      )
      .agg(
        sum("sales_units").alias("total_units"),
        sum("sales_dollars").alias("total_dollars"),
        round(avg("ros_units_per_week"), 2).alias("avg_ros_per_week")
      )
      .orderBy(col("sub_category"), desc("total_dollars"))

    println(s"🔒 SECURE CLIENT DATA MART (Delivered to $CLIENT_MANUFACTURER):")
    finalClientDeliveryMart.select(
      "sub_category", "brand_name_masked", "upc_key_masked", 
      "segmentation", "total_dollars", "avg_ros_per_week", "assortment_action"
    ).show(truncate = false)

    println("\n==========================================================")
    println("✅ PIPELINE COMPLETE. DATA MASKED AND SECURED.")
    println("==========================================================")

    spark.stop()
  }
}
