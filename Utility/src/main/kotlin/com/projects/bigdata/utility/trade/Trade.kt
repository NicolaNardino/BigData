package com.projects.bigdata.utility.trade

import java.io.Serializable
import java.math.BigDecimal

class Trade(val symbol: String, val direction: Direction, val quantity: Int, val price: BigDecimal, val exchange: Exchange) : Serializable {
    companion object {
        private const val serialVersionUID = 1L
    }

    //Needed for the JSON deserialization.
    private constructor() : this("", Direction.Buy, 0, BigDecimal(0.0), Exchange.EUREX)

    override fun toString(): String {
        return "Trade(symbol='$symbol', direction=$direction, quantity=$quantity, price=$price, exchange=$exchange)"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Trade

        if (symbol != other.symbol) return false
        if (direction != other.direction) return false
        if (quantity != other.quantity) return false
        if (price != other.price) return false
        if (exchange != other.exchange) return false

        return true
    }

    override fun hashCode(): Int {
        var result = symbol.hashCode()
        result = 31 * result + direction.hashCode()
        result = 31 * result + quantity
        result = 31 * result + price.hashCode()
        result = 31 * result + exchange.hashCode()
        return result
    }
}