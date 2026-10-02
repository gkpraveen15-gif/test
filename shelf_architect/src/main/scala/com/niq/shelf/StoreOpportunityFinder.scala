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
      ("STR_001", "UPC_101", 2400.0), // Underperforming
      ("STR_001", "UPC_102", 600.0),  // Overperforming
      ("STR_002", "UPC_101", 3000.0), // On target
      ("STR_003", "UPC_101", 500.0)   // Terrible performance
    ).toDF("store_id", "product_id_masked", "actual_dollars")

    // ==========================================
    // 2. INGEST STORE CLUSTER MASTER DATA
    // ==========================================
    println("▶ [PHASE 2] Loading Store Cluster Definitions...")
    println("  - Defining clusters: Dynamic collections of stores based on demographics, NOT just geography.")
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
      ("STR_001", "UPC_101", 3000.0), // Expected 3000, Actual 2400 -> 600 Gap
      ("STR_001", "UPC_102", 500.0),  // Expected 500, Actual 600 -> Beating forecast!
      ("STR_002", "UPC_101", 3000.0), // Expected 3000, Actual 3000 -> Perfect execution
      ("STR_003", "UPC_101", 2000.0)  // Expected 2000, Actual 500 -> Massive 1500 Gap
    ).toDF("store_id", "product_id_masked", "forecasted_dollars")

    // ==========================================
    // 4. BUSINESS LOGIC: OPPORTUNITY GAP CALCULATION
    // ==========================================
    println("\n▶ [PHASE 4] CALCULATING STORE-LEVEL OPPORTUNITY GAPS...")
    val masterDF = actualSalesDF
      .join(forecastDF, Seq("store_id", "product_id_masked"))
      .join(dimCluster, "store_id")

    // Calculate Opportunity = Forecast - Actual. (If negative, we beat the forecast so Gap is 0)
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

    println("\n====================================================================================================")
    println("✅ STORE OPPORTUNITY PIPELINE COMPLETE.")
    println("====================================================================================================\n")

    spark.stop()
  }
}
