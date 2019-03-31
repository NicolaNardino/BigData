package com.projects.bigdata.data_streaming.com.projects.bigdata.data_streaming.cassandra

import com.projects.bigdata.data_streaming.cassandra.CassandraManager
import com.projects.bigdata.utility.trade.Trade
import org.slf4j.LoggerFactory

class CassandraStore(private val cassandraManager: CassandraManager) {

    fun storeTrade (trade: Trade) {
        val sb = StringBuilder("insert into spark_data.trade (symbol, direction, quantity, price, exchange, timestamp) ")
                .append("values ('").append(trade.symbol).append("', '").append(trade.direction).append("', ").append(trade.quantity)
                .append(", ").append(trade.price).append(", '").append(trade.exchange).append("', '").append(trade.timestamp).append("')")
        val insertQuery = sb.toString()
        logger.debug("Executing $insertQuery")
        cassandraManager.session.execute(insertQuery)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(CassandraStore::class.java)
    }
}