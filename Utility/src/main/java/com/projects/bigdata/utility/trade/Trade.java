package com.projects.bigdata.utility.trade;

import java.io.Serializable;
import java.math.BigDecimal;

public final class Trade implements Serializable {
	private static final long serialVersionUID = 1L;

	private final String symbol;
	private final BigDecimal price;
	private final Direction direction;
	private final int quantity; //not considering fractional shares.
	private final Exchange exchange;

	public Trade(final String symbol, final Direction direction, final int quantity, final BigDecimal price, final Exchange exchange) {
		this.symbol = symbol;
		this.price = price;
		this.direction = direction;
		this.quantity = quantity;
		this.exchange = exchange;
	}

	/**Needed for the JSON deserialization.
	 * */
	@SuppressWarnings("unused")
	private Trade() {
		symbol = "";
		price = new BigDecimal(0.0);
		direction = Direction.Buy;
		quantity = 1;
		exchange = Exchange.EUREX;
	}

	public String getSymbol() {
		return symbol;
	}

	public BigDecimal getPrice() {
		return price;
	}

	public Direction getDirection() {
		return direction;
	}

	public int getQuantity() {
		return quantity;
	}

	public Exchange getExchange() {
		return exchange;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((direction == null) ? 0 : direction.hashCode());
		result = prime * result + ((exchange == null) ? 0 : exchange.hashCode());
		result = prime * result + ((price == null) ? 0 : price.hashCode());
		result = prime * result + quantity;
		result = prime * result + ((symbol == null) ? 0 : symbol.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Trade other = (Trade) obj;
		if (direction != other.direction)
			return false;
		if (exchange != other.exchange)
			return false;
		if (price == null) {
			if (other.price != null)
				return false;
		} else if (!price.equals(other.price))
			return false;
		if (quantity != other.quantity)
			return false;
		if (symbol == null) {
			if (other.symbol != null)
				return false;
		} else if (!symbol.equals(other.symbol))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Trade [symbol=" + symbol + ", price=" + price + ", direction=" + direction + ", quantity=" + quantity
				+ ", exchange=" + exchange + "]";
	}
}