package com.projects.bigdata.spark_streaming

import com.fasterxml.jackson.databind.ObjectMapper
import com.projects.bigdata.spark_streaming.utility.StreamingAppParameters
import com.projects.bigdata.utility.trade.Trade
import org.apache.spark.api.java.Optional
import org.apache.spark.streaming.State
import org.apache.spark.streaming.StateSpec
import org.slf4j.LoggerFactory
import scala.Tuple2

import java.io.Serializable
import java.util.ArrayList
import java.util.stream.Collectors

/**
 * It aggregates two streams of JSON represented `Trade2`s and then applies stateful aggregations using mapWithState.
 *
 */
class TradesAnalytics(sap: StreamingAppParameters) : AbstractStreaming(sap) {

    override fun process() {
        val unionStream = streamingContext.socketTextStream(sap.dataStreamHost1, sap.dataStreamPort1).map{ convertJsonToTrade(it) }
                .union(streamingContext.socketTextStream(sap.dataStreamHost2, sap.dataStreamPort2).map{ convertJsonToTrade(it) })
        val symbolToTrade = unionStream.mapToPair { t -> Tuple2(t.symbol, t) }
        val mapWithState = symbolToTrade.mapWithState<ArrayList<Trade>, Tuple2<String, Double>>(StateSpec.function(::tradesAggregation))
        mapWithState.foreachRDD { rdd -> rdd.collect().stream().map { it.toString() }.forEach{ logger.info(it) } }

        startStreamingAndAwaitTerminationOrTimeout()
    }

    companion object : Serializable {
        private val logger = LoggerFactory.getLogger(TradesAnalytics::class.java)
        private val mapper = ObjectMapper()

        private fun tradesAggregation(symbol: String, currentTrade: Optional<Trade>, state: State<ArrayList<Trade>>): Tuple2<String, Double> {
            val tradesState = if (state.exists()) state.get() else ArrayList()
            val sb = StringBuilder()
            sb.append("Current Trade:" + currentTrade.orNull() + "\n").append("Current Trades:\n" + tradesState.stream().map { it.toString() }.collect(Collectors.joining("\n")) + "\n")
            if (currentTrade.isPresent)
                tradesState.add(currentTrade.get())
            val priceAverage = tradesState.stream().mapToDouble { it.price.toDouble() }.average().orElse(0.0)
            state.update(tradesState)
            val tuple2 = Tuple2(symbol, priceAverage)
            sb.append("Result: $tuple2")
            logger.info(sb.toString())
            return tuple2
        }

        private fun convertJsonToTrade(jsonTrade: String): Trade {
            return mapper.readValue(jsonTrade, Trade::class.java)
        }
    }
}