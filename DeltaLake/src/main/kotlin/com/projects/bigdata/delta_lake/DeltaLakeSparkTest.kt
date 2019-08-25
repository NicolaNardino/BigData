package com.projects.bigdata.delta_lake

import org.apache.spark.sql.SparkSession

fun main () {
    val outputFolder = "~/Projects/DeltaLake/Tables/spark-range-delta-table"
    val spark = SparkSession.builder().appName("DeltaLateTest").master("local[*]").orCreate
    spark.range(0,100).write().format("delta").save(outputFolder);
    val df = spark.read().format("delta").load(outputFolder)
    df.show()
}