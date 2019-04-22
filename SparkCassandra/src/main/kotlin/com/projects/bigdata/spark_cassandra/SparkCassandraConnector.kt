package com.projects.bigdata.spark_cassandra

import com.projects.bigdata.utility.getApplicationProperties
import com.projects.bigdata.utility.trade.Direction
import com.projects.bigdata.utility.trade.Exchange
import com.projects.bigdata.utility.trade.Trade
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import scala.Serializable
import java.math.BigDecimal
import java.util.UUID


class SparkCassandraConnector {
    fun connect() {
        with (getApplicationProperties("application.properties")) {
            val sparkSession = SparkSession.builder().master("local[*]").appName("CassandraSparkConnector")
                    .config("spark.cassandra.connection.host", getProperty("cassandra.node"))
                    .config("spark.cassandra.connection.port", getProperty("cassandra.port")).orCreate
            val cassandraConfig = mapOf("table" to getProperty("cassandra.table"), "keyspace" to getProperty("cassandra.keyspace"))
            val dataset: Dataset<Row> = sparkSession.read().format("org.apache.spark.sql.cassandra").options(cassandraConfig).load()
            //dataset.printSchema()
            dataset.toJavaRDD().map(::call).collect().forEach(::println)
        }
    }

    companion object : Serializable {
        private fun call(row: Row?): Trade {
            val notNullRow : Row = row!!
            return Trade(notNullRow.getAs<String>("symbol"), Direction.valueOf(notNullRow.getAs<String>("direction")), notNullRow.getAs<Int>("quantity"),
                    BigDecimal.valueOf(notNullRow.getAs<Double>("price")),
                    Exchange.valueOf(notNullRow.getAs<String>("exchange")), UUID.fromString(notNullRow.getAs<String>("timestamp")))
        }
    }
}


fun main() = SparkCassandraConnector().connect()