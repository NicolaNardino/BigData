package com.projects.bigdata.spark_streaming;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.projects.bigdata.spark_streaming.utility.StreamingAppParameters;
import com.projects.bigdata.utility.trade.Trade;

import scala.Tuple2;

/**
 * It aggregates two streams of JSON represented {@code Trade}s and then applies stateful aggregations using mapWithState.
 * 
 * */
public final class TradesAnalytics extends AbstractStreaming {
	private static final Logger logger = LoggerFactory.getLogger(TradesAnalytics.class);
	private static final ObjectMapper mapper = new ObjectMapper();

    public TradesAnalytics(final StreamingAppParameters sap) {
    	super(sap);
    }
    
    public void process() throws InterruptedException {    	
    	final JavaDStream<Trade> unionStream = streamingContext.socketTextStream(dataStreamHost1, dataStreamPort1).map(TradesAnalytics::convertJsonToTrade)
    			.union(streamingContext.socketTextStream(dataStreamHost2, dataStreamPort2).map(TradesAnalytics::convertJsonToTrade));
    	final JavaPairDStream<String, Trade> symbolToTrade = unionStream.mapToPair(t -> new Tuple2<String, Trade>(t.getSymbol(), t));
    	final JavaMapWithStateDStream<String, Trade, List<Trade>, Tuple2<String, Double>> mapWithState = symbolToTrade.mapWithState(StateSpec.function(new StatefulAggregation()));
    	mapWithState.print();
    	startStreamingAndAwaitTerminationOrTimeout(processingTimeout);
    }
    
    private static class StatefulAggregation implements Function3<String, Optional<Trade>, State<List<Trade>>, Tuple2<String, Double>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, Double> call(final String symbol, Optional<Trade> currentTrade, State<List<Trade>> state) throws Exception {         
            final List<Trade> tradesState = state.exists() ? state.get() : new ArrayList<>(); 
            logger.info("Current Trade:\n"+currentTrade.orNull());
            logger.info("Current Trades:\n"+tradesState.stream().map(t -> t.toString()).collect(Collectors.joining("\n")));
            if (currentTrade.isPresent()) 
            	tradesState.add(currentTrade.get());  
            final Double priceAverage = tradesState.stream().mapToDouble(t -> t.getPrice().doubleValue()).average().orElse(0.0); 
            logger.info("Current Price Average: "+priceAverage);
            state.update(tradesState); 
            return new Tuple2<String, Double>(symbol, priceAverage); 
		}
    }
    
    private static Trade convertJsonToTrade(final String jsonTrade) throws JsonParseException, JsonMappingException, IOException {
    	return mapper.readValue(jsonTrade, Trade.class);
    }
 }