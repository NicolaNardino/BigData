package com.projects.bigdata.data_streaming;

import static java.util.concurrent.ThreadLocalRandom.current;

import java.math.BigDecimal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.projects.bigdata.utility.Utility;
import com.projects.bigdata.utility.trade.Direction;
import com.projects.bigdata.utility.trade.Exchange;
import com.projects.bigdata.utility.trade.Trade;

/**
 * 
 * It sends over a socket randomly <code>Trade</code> instances in JSON format, like the following:
 * 		{"symbol":"ACN","price":475.5955733771751283711637370288372039794921875,"direction":"Sell","quantity":44,"exchange":"FTSE"}
 * 		{"symbol":"AGN","price":378.637626500618580394075252115726470947265625,"direction":"Buy","quantity":65,"exchange":"FTSE"}
 * 		{"symbol":"MSFT","price":486.895279745447396635427139699459075927734375,"direction":"Buy","quantity":89,"exchange":"NASDAQ"}		
 * 
 * */
public final class TradeStreamingTCPServer extends AbstractDataStreamingTCPServer {
	private static final Logger logger = LoggerFactory.getLogger(TradeStreamingTCPServer.class);
	
	private final ObjectMapper mapper;
	public TradeStreamingTCPServer(final int port, final int messageSendDelayMilliSeconds) {
		super(port, messageSendDelayMilliSeconds);
		mapper = new ObjectMapper();
	}

	/**
	 * @return JSON representations of Trade objects.
	 * */
	public String buildLine() {
		try {
			final var trade = new Trade(Utility.Symbols.get(current().nextInt(Utility.Symbols.size())), getRandomEnumValue(Direction.class), current().nextInt(1, 100), 
					new BigDecimal(current().nextDouble(1.0, 999.0)), getRandomEnumValue(Exchange.class));
			logger.info("Trade: " + trade);
			return mapper.writeValueAsString(trade).toString();
		} catch (final JsonProcessingException e) {
			logger.warn("Unable to build JSON string.", e);
			return "";
		}
	}
	
	private static <T extends Enum<?>> T getRandomEnumValue(final Class<T> clazz){
        return clazz.getEnumConstants()[current().nextInt(clazz.getEnumConstants().length)];
    }
}