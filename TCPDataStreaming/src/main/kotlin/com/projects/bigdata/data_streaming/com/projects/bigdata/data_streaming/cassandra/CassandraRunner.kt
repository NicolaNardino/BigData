package com.projects.bigdata.data_streaming.com.projects.bigdata.data_streaming.cassandra

import com.projects.bigdata.data_streaming.cassandra.CassandraManager
import com.projects.bigdata.utility.trade.Direction
import com.projects.bigdata.utility.trade.Exchange
import com.projects.bigdata.utility.trade.Trade
import java.math.BigDecimal

fun main() {
    val cassandraManager = CassandraManager("127.0.0.1", 9042)
    var trades = listOf(Trade("1", Direction.Buy, 1, BigDecimal.TEN, Exchange.FTSE),
            Trade("2", Direction.Buy, 1, BigDecimal.ONE, Exchange.EUREX),
            Trade("3", Direction.Sell, 1, BigDecimal.ZERO, Exchange.NASDAQ))
    cassandraManager.use {
        val cassandraStore = CassandraStore(it)
        trades.forEach {trade ->  cassandraStore.storeTrade(trade)}
    }
}