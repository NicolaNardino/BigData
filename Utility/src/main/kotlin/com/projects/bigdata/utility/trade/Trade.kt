package com.projects.bigdata.utility.trade

import java.io.Serializable
import java.math.BigDecimal

data class Trade(val symbol: String, val direction: Direction, val quantity: Int, val price: BigDecimal, val exchange: Exchange) : Serializable, Comparable<Trade> {
    //Needed for the JSON deserialization.
    private constructor() : this("", Direction.Buy, 0, BigDecimal(0.0), Exchange.EUREX)

    override fun compareTo(other: Trade) = compareValuesBy(this, other, Trade::symbol, Trade::direction, Trade::exchange)

    companion object {
        private const val serialVersionUID = 1L
    }
}