package com.projects.bigdata.spark_cassandra

import com.projects.bigdata.utility.getApplicationProperties
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession


class SparkCassandraConnector {

    fun connect() {
        with (getApplicationProperties("application.properties")) {
            val sparkSession = SparkSession.builder().master("local[*]").appName("CassandraSparkConnector")
                    .config("spark.cassandra.connection.host", getProperty("cassandra.node"))
                    .config("spark.cassandra.connection.port", getProperty("cassandra.port")).getOrCreate()
            val cassandraConfig = mapOf("table" to getProperty("cassandra.table"), "keyspace" to getProperty("cassandra.keyspace"))
            val dataset: Dataset<Row> = sparkSession.read().format("org.apache.spark.sql.cassandra").options(cassandraConfig).load()
            dataset.printSchema()
            dataset.show()
        }
    }
}

fun main() {
    SparkCassandraConnector().connect()
}