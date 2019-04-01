package com.projects.bigdata.data_streaming.com.projects.bigdata.data_streaming.cassandra

import com.projects.bigdata.data_streaming.cassandra.CassandraManager
import com.projects.bigdata.utility.trade.Trade
import org.slf4j.LoggerFactory

class CassandraDataStore(private val cassandraManager: CassandraManager) : ICassandraDataStore {

    override fun storeTrade (trade: Trade) {
        val sb = StringBuilder("insert into spark_data.trade (symbol, direction, quantity, price, exchange, timestamp) ")
                .append("values ('").append(trade.symbol).append("', '").append(trade.direction).append("', ").append(trade.quantity)
                .append(", ").append(trade.price).append(", '").append(trade.exchange).append("', '").append(trade.timestamp).append("')")
        val insertQuery = sb.toString()
        logger.debug("Executing $insertQuery")
        cassandraManager.session.execute(insertQuery)
    }

    override fun createKeyspaceIfNotExists(keyspaceName: String, replicationStrategy: String, replicationFactor: Int) {
        val sb = StringBuilder("create keyspace if not exists ").append(keyspaceName).append(" WITH replication = {")
                .append("'class':'").append(replicationStrategy)
                .append("','replication_factor':").append(replicationFactor)
                .append("};")
        val keyspaceQuery = sb.toString()
        logger.debug("Executing $keyspaceQuery")
        cassandraManager.session.execute(keyspaceQuery)
    }

    override fun createTableIfNotExists(tableDefinition: String) {
        logger.debug("Executing $tableDefinition")
        cassandraManager.session.execute(tableDefinition)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(CassandraDataStore::class.java)
    }
}