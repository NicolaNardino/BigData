package com.projects.bigdata.cassandra_microservice

import com.projects.bigdata.utility.trade.Direction
import com.projects.bigdata.utility.trade.Exchange
import com.projects.bigdata.utility.trade.Trade
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping

@Controller
@RequestMapping("/cassandra")
class CassandraRestController {

    @Autowired
    lateinit var cassandraTradeRepository: CassandraTradeRepository

    @GetMapping("/getAllTrades", produces = [MediaType.APPLICATION_JSON_VALUE])
    fun getAllTrades(): ResponseEntity<List<Trade>> = ResponseEntity(cassandraTradeRepository.findAll(), HttpStatus.OK)

    @GetMapping("/getTradesByExchange/{exchange}", produces = [MediaType.APPLICATION_JSON_VALUE])
    fun getTradesByExchange(@PathVariable exchange: Exchange): ResponseEntity<List<Trade>> =
            ResponseEntity(cassandraTradeRepository.findByExchange(exchange), HttpStatus.OK)

    @GetMapping("/getTradesByExchangeAndDirection/{exchange}/{direction}", produces = [MediaType.APPLICATION_JSON_VALUE])
    fun getTradesByExchangeAndDirection(@PathVariable exchange: Exchange, @PathVariable direction: Direction): ResponseEntity<List<Trade>> =
            ResponseEntity(cassandraTradeRepository.findByExchangeAndDirection(exchange, direction), HttpStatus.OK)

    @GetMapping("/getTradesByExchangeAndDirectionAndSymbol/{exchange}/{direction}/{symbol}", produces = [MediaType.APPLICATION_JSON_VALUE])
    fun getTradesByExchangeAndDirection(@PathVariable exchange: Exchange, @PathVariable direction: Direction, @PathVariable symbol: String): ResponseEntity<List<Trade>> =
            ResponseEntity(cassandraTradeRepository.findByExchangeAndDirectionAndSymbol(exchange, direction, symbol), HttpStatus.OK)
}