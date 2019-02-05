package com.projects.bigdata.spark_streaming;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.Optional;
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
 * It aggregates two streams of JSON represented {@code Trade2}s and then applies stateful aggregations using mapWithState.
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
        final JavaPairDStream<String, Trade> symbolToTrade = unionStream.mapToPair(t -> new Tuple2(t.getSymbol(), t));
        final JavaMapWithStateDStream<String, Trade, List<Trade>, Tuple2<String, Double>> mapWithState = symbolToTrade.mapWithState(StateSpec.function(TradesAnalytics::tradesAggregation));
        //mapWithState.print();
        mapWithState.foreachRDD(rdd -> rdd.collect().stream().map(t -> t.toString()).forEach(logger::info));

        startStreamingAndAwaitTerminationOrTimeout();
    }

    private static Tuple2<String, Double> tradesAggregation(final String symbol, Optional<Trade> currentTrade, State<List<Trade>> state) throws Exception {
        final List<Trade> tradesState = state.exists() ? state.get() : new ArrayList<>();
        final StringBuilder sb = new StringBuilder();
        sb.append("Current Trade:"+currentTrade.orNull()+"\n").append("Current Trades:\n"+tradesState.stream().map(t -> t.toString()).collect(Collectors.joining("\n"))+"\n");
        if (currentTrade.isPresent())
            tradesState.add(currentTrade.get());
        final Double priceAverage = tradesState.stream().mapToDouble(t -> t.getPrice().doubleValue()).average().orElse(0.0);
        state.update(tradesState);
        final Tuple2<String, Double> tuple2 = new Tuple2<String, Double>(symbol, priceAverage);
        sb.append("Result: "+tuple2);
        logger.info(sb.toString());
        return tuple2;
    }

    private static Trade convertJsonToTrade(final String jsonTrade) throws IOException {
        return mapper.readValue(jsonTrade, Trade.class);
    }
}