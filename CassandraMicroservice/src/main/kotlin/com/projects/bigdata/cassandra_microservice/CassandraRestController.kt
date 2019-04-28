package com.projects.bigdata.cassandra_microservice

import com.projects.bigdata.utility.trade.Direction
import com.projects.bigdata.utility.trade.Exchange
import com.projects.bigdata.utility.trade.Trade
import io.swagger.annotations.ApiOperation
import io.swagger.annotations.ApiParam
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.*

@Controller
@RequestMapping("/cassandra")
class CassandraRestController : ICassandraRestController {

    @Autowired
    lateinit var cassandraTradeRepository: CassandraTradeRepository

    @ApiOperation(value = "Get all trades.")
    @GetMapping("/getAllTrades", produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getAllTrades(): ResponseEntity<List<Trade>> = ResponseEntity.ok(cassandraTradeRepository.findAll())

    @ApiOperation(value = "Get all trades by exchange.")
    @GetMapping("/getTradesByExchange/{exchange}", produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getTradesByExchange(@ApiParam(value = "Exchange.", required = true)
                            @PathVariable exchange: Exchange): ResponseEntity<List<Trade>> = ResponseEntity.ok(cassandraTradeRepository.findByExchange(exchange))

    @ApiOperation(value = "Get all trades by exchange and direction (buy/ sell).")
    @GetMapping("/getTradesByExchangeAndDirection/{exchange}/{direction}", produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getTradesByExchangeAndDirection(
            @ApiParam(value = "Exchange.", required = true)
            @PathVariable exchange: Exchange,
            @ApiParam(value = "Direction.", required = true)
            @PathVariable direction: Direction): ResponseEntity<List<Trade>> =
            ResponseEntity.ok(cassandraTradeRepository.findByExchangeAndDirection(exchange, direction))

    @ApiOperation(value = "Get all trades by exchange, direction and symbol.")
    @GetMapping("/getTradesByExchangeAndDirectionAndSymbol/{exchange}/{direction}/{symbol}", produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getTradesByExchangeAndDirection(
            @ApiParam(value = "Exchange.", required = true)
            @PathVariable exchange: Exchange,
            @ApiParam(value = "Direction.", required = true)
            @PathVariable direction: Direction,
            @ApiParam(value = "Symbol.", required = true)
            @PathVariable symbol: String): ResponseEntity<List<Trade>> =
            ResponseEntity.ok(cassandraTradeRepository.findByExchangeAndDirectionAndSymbol(exchange, direction, symbol))

    @ApiOperation(value = "Add a Trade to the Cassandra Repository.")
    @PostMapping("/addTrade", consumes = [MediaType.APPLICATION_JSON_VALUE])
    override fun insertTrade(
            @ApiParam(value = "New Trade.", required = true)
            @RequestBody trade: Trade) : ResponseEntity<String> {
        cassandraTradeRepository.insert(trade)
        return ResponseEntity.ok("Added $trade")
    }
}