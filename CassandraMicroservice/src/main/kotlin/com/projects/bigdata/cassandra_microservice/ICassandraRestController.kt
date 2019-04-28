package com.projects.bigdata.cassandra_microservice

import com.projects.bigdata.utility.trade.Direction
import com.projects.bigdata.utility.trade.Exchange
import com.projects.bigdata.utility.trade.Trade
import org.springframework.http.ResponseEntity

interface ICassandraRestController {
    fun getAllTrades(): ResponseEntity<List<Trade>>

    fun getTradesByExchange(exchange: Exchange): ResponseEntity<List<Trade>>

    fun getTradesByExchangeAndDirection(exchange: Exchange, direction: Direction): ResponseEntity<List<Trade>>

    fun getTradesByExchangeAndDirection(exchange: Exchange, direction: Direction, symbol: String): ResponseEntity<List<Trade>>

    fun insertTrade(trade: Trade) : ResponseEntity<String>
}