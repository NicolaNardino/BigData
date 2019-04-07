package com.projects.bigdata.cassandra_microservice

import com.projects.bigdata.utility.trade.Direction
import com.projects.bigdata.utility.trade.Exchange
import com.projects.bigdata.utility.trade.Trade
import org.springframework.data.cassandra.repository.CassandraRepository
import org.springframework.stereotype.Repository
import java.sql.Timestamp

@Repository
interface CassandraTradeRepository: CassandraRepository<Trade, Timestamp> {
    fun findByExchange(exchange: Exchange): List<Trade>
    fun findByExchangeAndDirection(exchange: Exchange, direction: Direction): List<Trade>
    fun findByExchangeAndDirectionAndSymbol(exchange: Exchange, direction: Direction, symbol: String): List<Trade>
}