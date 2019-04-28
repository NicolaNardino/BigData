package com.projects.bigdata.spark_cassandra

import com.projects.bigdata.utility.getApplicationProperties
import com.projects.bigdata.utility.trade.Direction
import com.projects.bigdata.utility.trade.Exchange
import com.projects.bigdata.utility.trade.Trade
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import scala.Serializable
import scala.Tuple2
import scala.Tuple3
import java.math.BigDecimal
import java.util.UUID


class TradesAnalytics : ITradesAnalytics {

    private fun getDataSet(): Dataset<Row>? {
        with (getApplicationProperties("application.properties")) {
            val sparkSession = SparkSession.builder().master("local[*]").appName("CassandraSparkConnector")
                    .config("spark.cassandra.connection.host", getProperty("cassandra.node"))
                    .config("spark.cassandra.connection.port", getProperty("cassandra.port")).orCreate
            val cassandraConfig = mapOf("table" to getProperty("cassandra.table"), "keyspace" to getProperty("cassandra.keyspace"))
            return sparkSession.read().format("org.apache.spark.sql.cassandra").options(cassandraConfig).load()
        }
    }

    override fun averageQuantityPerSymbolDirectionExchange() : Map<Tuple3<String, Direction, Exchange>, Double>  {
        val dataset: Dataset<Row>? = getDataSet()
        //dataset.printSchema()
        val tradeRDD: JavaRDD<Trade> = dataset?.toJavaRDD()!!.map(::call)
        val quantityBySymbolDirectionExchange: JavaPairRDD<Tuple3<String, Direction, Exchange>, Int> = tradeRDD.mapToPair { trade -> Tuple2(Tuple3(trade.symbol, trade.direction, trade.exchange), trade.quantity) }
        //in order to compute the average, prepare a value Tuple2 of actual quantity and 1, so that quantity will be later summed up and used as numerator, while 1 will be used as denominator
        val mapValues: JavaPairRDD<Tuple3<String, Direction, Exchange>, Tuple2<Int, Int>> = quantityBySymbolDirectionExchange.mapValues { value -> Tuple2(value, 1) }
        val summedUpQuantityAndCount: JavaPairRDD<Tuple3<String, Direction, Exchange>, Tuple2<Int, Int>> = mapValues.reduceByKey { t1, t2 -> Tuple2(t1._1 + t2._1, t1._2 + t2._2) }
        val averageQuantityPerSymbolDirectionExchangeRDD: JavaPairRDD<Tuple3<String, Direction, Exchange>, Double> = summedUpQuantityAndCount.mapValues { t -> (t._1 / t._2).toDouble() }
        val averageQuantityPerSymbolDirectionExchange: Map<Tuple3<String, Direction, Exchange>, Double> = averageQuantityPerSymbolDirectionExchangeRDD.collectAsMap()
        println(averageQuantityPerSymbolDirectionExchange)
        return averageQuantityPerSymbolDirectionExchange
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