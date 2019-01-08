package com.projects.bigdata.utility.trade;

import java.math.BigDecimal;

public final class Trade {
	private final String symbol;
	private final BigDecimal price;
	private final Direction direction;
	private final int quantity; //not considering fractional shares.
	private final Exchange exchange;
	
	/**
	 * @param symbol
	 * @param price
	 * @param direction
	 * @param quantity
	 * @param exchange
	 */
	public Trade(final String symbol, final Direction direction, int quantity, final BigDecimal price, final Exchange exchange) {
		this.symbol = symbol;
		this.price = price;
		this.direction = direction;
		this.quantity = quantity;
		this.exchange = exchange;
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
	public String toString() {
		return "Trade [symbol=" + symbol + ", price=" + price + ", direction=" + direction + ", quantity=" + quantity
				+ ", exchange=" + exchange + "]";
	}
}
