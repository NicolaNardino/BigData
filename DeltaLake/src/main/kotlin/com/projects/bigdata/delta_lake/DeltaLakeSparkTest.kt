package com.projects.bigdata.delta_lake

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

fun main () {
    val outputFolder = "DeltaTables/spark-range-delta-table"
    val spark = SparkSession.builder().appName("DeltaLateTest").master("local[*]").orCreate
    spark.range(0,100).write().format("delta").mode(SaveMode.Overwrite).save(outputFolder);
    spark.range(0,1000).write().format("delta").mode(SaveMode.Append).save(outputFolder);
    val loadedDs = spark.read().format("delta").load(outputFolder)
    loadedDs.show()
}