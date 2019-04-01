package com.projects.bigdata.data_streaming.com.projects.bigdata.data_streaming.cassandra

import com.projects.bigdata.utility.trade.Trade

interface ICassandraDataStore {
    fun storeTrade (trade: Trade)
    fun createKeyspaceIfNotExists(keyspaceName: String, replicationStrategy: String, replicationFactor: Int)
    fun createTableIfNotExists(tableDefinition: String)
}