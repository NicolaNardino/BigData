package com.projects.bigdata.data_streaming.utility

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.projects.bigdata.data_streaming.cassandra.CassandraManager
import com.projects.bigdata.data_streaming.com.projects.bigdata.data_streaming.cassandra.CassandraDataStore
import com.projects.bigdata.utility.*
import com.projects.bigdata.utility.trade.Direction
import com.projects.bigdata.utility.trade.Exchange
import com.projects.bigdata.utility.trade.Trade
import org.slf4j.LoggerFactory

import java.math.BigDecimal
import java.util.concurrent.ThreadLocalRandom
import java.util.stream.Collectors
import java.util.stream.IntStream

import java.util.concurrent.ThreadLocalRandom.current

/**
 * `Supplier` implementations to be passed in to `DataStreamingTCPServer`, when it comes to building a string to be sent over the network through `Socket`s.
 *
 */
object StreamingLineSupplier {
    private val logger = LoggerFactory.getLogger(StreamingLineSupplier::class.java)
    private val mapper = ObjectMapper()
    private val cassandraStore = CassandraDataStore(CassandraManager())

    /**
     * It generates a phrase made up by randomly generated words.
     *
     * @return Randomly generated phrase.
     */
    fun randomPhrase(): String {
        val maxNrWordsPerPhrase = 10
        val maxNrWordPostfix = 10
        return IntStream.range(1, ThreadLocalRandom.current().nextInt(1, maxNrWordsPerPhrase) + 1).
                mapToObj { "Word" + ThreadLocalRandom.current().nextInt(1, maxNrWordPostfix) }.collect(Collectors.joining(" "))
    }

    /**
     * It randomly generates a `Trade2` obejct and converts it to JSON.
     *
     * @return JSON representations of Trade object.
     */
    fun randomTrade(): String {
        return try {
            val trade = Trade(Symbols[current().nextInt(Symbols.size)], getRandomEnumValue(Direction::class.java), current().nextInt(1, 100),
                    BigDecimal(current().nextDouble(1.0, 999.0)), getRandomEnumValue(Exchange::class.java))
            cassandraStore.storeTrade(trade)
            mapper.writeValueAsString(trade)
        } catch (e: JsonProcessingException) {
            logger.warn("Unable to build JSON string.", e)
            ""
        }

    }

    private fun <T : Enum<*>> getRandomEnumValue(clazz: Class<T>): T {
        return clazz.enumConstants[current().nextInt(clazz.enumConstants.size)]
    }
}