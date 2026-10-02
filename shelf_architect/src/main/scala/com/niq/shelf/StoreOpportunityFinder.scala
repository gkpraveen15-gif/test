package com.niq.shelf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object StoreOpportunityFinder {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NIQ Store Opportunity Finder")
      .master("local[*]")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    println("\n====================================================================================================")
    println("🌟 EXECUTIVE SUMMARY: STORE-LEVEL GROWTH OPPORTUNITY & CLUSTER ANALYSIS")
    println("====================================================================================================")
    println("▶ THE BUSINESS PROBLEM:")
    println("  While the Shelf Architect tells us WHAT to stock, we now need to know WHERE we are underperforming.")
    println("  This application ingests the Shelf Output and compares Actual Sales vs Machine Learning Forecasts.")
    println("  Stores are grouped into 'Clusters' (e.g., High-Income Urban) categorized as Focus or Competitive.")
    println("  We will pinpoint the exact stores leaving money on the table (The Opportunity Gap).")
    println("====================================================================================================\n")

    // ==========================================
    // 1. INGEST MOCKED SHELF ARCHITECT OUTPUT
    // ==========================================
    println("▶ [PHASE 1] Loading Actual Sales (Ingesting output from the Shelf Optimizer)...")
    val actualSalesDF = Seq(
      ("STR_001", "UPC_101 (Premium Energy)", 2400.0), 
      ("STR_001", "UPC_102 (Comp Energy)", 600.0),  
      ("STR_001", "UPC_201 (Organic Chips)", 15.0),    // Very slow mover
      ("STR_002", "UPC_101 (Premium Energy)", 3000.0), 
      ("STR_003", "UPC_101 (Premium Energy)", 500.0)   // Terrible performance
    ).toDF("store_id", "product_id_masked", "actual_dollars")

    // ==========================================
    // 2. INGEST STORE CLUSTER MASTER DATA
    // ==========================================
    println("▶ [PHASE 2] Loading Store Cluster Definitions...")
    val dimCluster = Seq(
      ("STR_001", "Urban_Flagship", "Focus_Cluster"),
      ("STR_002", "Urban_Flagship", "Focus_Cluster"),
      ("STR_003", "Suburban_Value", "Competitive_Cluster")
    ).toDF("store_id", "cluster_name", "cluster_type")

    // ==========================================
    // 3. INGEST MACHINE LEARNING FORECASTS
    // ==========================================
    println("▶ [PHASE 3] Loading AI/ML Forecasted Sales Targets...")
    val forecastDF = Seq(
      ("STR_001", "UPC_101 (Premium Energy)", 3000.0), 
      ("STR_001", "UPC_102 (Comp Energy)", 500.0),  
      ("STR_001", "UPC_201 (Organic Chips)", 150.0),  
      ("STR_002", "UPC_101 (Premium Energy)", 3000.0),
      ("STR_003", "UPC_101 (Premium Energy)", 2000.0)  // Massive 1500 Gap
    ).toDF("store_id", "product_id_masked", "forecasted_dollars")

    // ==========================================
    // 4. BUSINESS LOGIC: OPPORTUNITY GAP CALCULATION
    // ==========================================
    println("\n▶ [PHASE 4] CALCULATING STORE-LEVEL OPPORTUNITY GAPS...")
    val masterDF = actualSalesDF
      .join(forecastDF, Seq("store_id", "product_id_masked"))
      .join(dimCluster, "store_id")

    val oppGapDF = masterDF
      .withColumn("opportunity_gap_dollars", 
        when($"forecasted_dollars" > $"actual_dollars", $"forecasted_dollars" - $"actual_dollars")
        .otherwise(0.0)
      )
      .withColumn("performance_status",
        when($"opportunity_gap_dollars" > 1000, "CRITICAL_INTERVENTION_NEEDED")
        .when($"opportunity_gap_dollars" > 0, "UNDERPERFORMING")
        .otherwise("EXCEEDING_TARGETS")
      )

    // ==========================================
    // 5. BUSINESS LOGIC: CLUSTER-LEVEL AGGREGATION
    // ==========================================
    println("▶ [PHASE 5] AGGREGATING MISSED OPPORTUNITIES BY CLUSTER & TYPE...")
    val clusterSummaryDF = oppGapDF
      .groupBy("cluster_name", "cluster_type")
      .agg(
        sum("actual_dollars").alias("total_actual_sales"),
        sum("forecasted_dollars").alias("total_forecasted_sales"),
        sum("opportunity_gap_dollars").alias("total_missed_opportunity")
      )
      .withColumn("forecast_achievement_pct", round(($"total_actual_sales" / $"total_forecasted_sales") * 100, 2))
      .orderBy(desc("total_missed_opportunity"))

    println("\n====================================================================================================")
    println("📊 CLUSTER LEVEL SUMMARY (Where is the overall business losing money?)")
    println("====================================================================================================")
    clusterSummaryDF.show(truncate = false)

    println("\n====================================================================================================")
    println("🎯 STORE-LEVEL ACTION PLAN (Targeted interventions based on Opportunity Gap)")
    println("====================================================================================================")
    oppGapDF.select(
      "store_id", "cluster_type", "product_id_masked", 
      "actual_dollars", "forecasted_dollars", "opportunity_gap_dollars", "performance_status"
    ).orderBy(desc("opportunity_gap_dollars")).show(truncate = false)

    // ==========================================
    // 6. NATURAL LANGUAGE GENERATION (NLG) READOUT
    // ==========================================
    println("\n====================================================================================================")
    println("🤖 AI-GENERATED EXECUTIVE READOUT FOR BRAND_A")
    println("====================================================================================================")
    
    // Dynamically calculate the worst execution miss using Spark Actions
    val worstMiss = oppGapDF.orderBy(desc("opportunity_gap_dollars")).first()
    val worstStore = worstMiss.getAs[String]("store_id")
    val worstGap = worstMiss.getAs[Double]("opportunity_gap_dollars")
    val worstUpc = worstMiss.getAs[String]("product_id_masked")
    val worstCluster = worstMiss.getAs[String]("cluster_type")
    
    // Dynamically calculate the worst performing product overall
    val worstProduct = actualSalesDF.groupBy("product_id_masked")
      .agg(sum("actual_dollars").alias("total_sales"))
      .orderBy(asc("total_sales")).first()
    val dropUpc = worstProduct.getAs[String]("product_id_masked")
    val dropSales = worstProduct.getAs[Double]("total_sales")

    println(s"▶ 1. EXECUTION FAILURES IN COMPETITIVE CLUSTERS:")
    println(s"   Our Store Opportunity algorithm caught a massive execution failure in the $worstCluster.")
    println(s"   $worstStore was forecasted to sell highly, but missed the target for $worstUpc by $$${worstGap}.")
    println(s"   ACTION: A $$${worstGap} gap indicates a severe out-of-stock or display issue. Dispatch a merchandiser immediately.")
    
    println(s"\n▶ 2. PORTFOLIO OPTIMIZATION:")
    println(s"   While your core items are strong, $dropUpc is generating extremely low sales velocity (Only $$${dropSales} total).")
    println(s"   ACTION: Mark $dropUpc as a DELIST_CANDIDATE. Voluntarily pull it during the next planogram reset")
    println(s"   and negotiate to replace that empty shelf slot with a secondary facing of your high-performing $worstUpc.")
    println("====================================================================================================\n")

    spark.stop()
  }
}
