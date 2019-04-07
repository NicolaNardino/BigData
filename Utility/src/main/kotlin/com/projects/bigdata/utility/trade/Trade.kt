package com.projects.bigdata.utility.trade

import com.datastax.driver.core.utils.UUIDs
import org.springframework.data.cassandra.core.mapping.PrimaryKey
import org.springframework.data.cassandra.core.mapping.Table
import java.io.Serializable
import java.math.BigDecimal
import java.util.*

@Table("trade")
data class Trade(val symbol: String, val direction: Direction, val quantity: Int, val price: BigDecimal, val exchange: Exchange, @PrimaryKey val timestamp: UUID = UUIDs.timeBased()) : Serializable, Comparable<Trade> {
    //Needed for the JSON deserialization.
    private constructor() : this("", Direction.Buy, 0, BigDecimal(0.0), Exchange.EUREX)

    override fun compareTo(other: Trade) = compareValuesBy(this, other, Trade::symbol, Trade::direction, Trade::exchange)

    companion object {
        private const val serialVersionUID = 1L
    }
}