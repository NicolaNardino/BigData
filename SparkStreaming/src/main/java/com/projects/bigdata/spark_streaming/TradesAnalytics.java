package com.projects.bigdata.spark_streaming;

import java.io.IOException;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.projects.bigdata.spark_streaming.utility.StreamingAppParameters;
import com.projects.bigdata.utility.trade.Trade;

public final class TradesAnalytics extends AbstractStreaming {
	private static final Logger logger = LoggerFactory.getLogger(TradesAnalytics.class);
	private static final ObjectMapper mapper = new ObjectMapper();

    public TradesAnalytics(final StreamingAppParameters sap) {
    	super(sap);
    }
    
    public void process() throws InterruptedException {    	
    	final JavaReceiverInputDStream<String> inputStream1 = streamingContext.socketTextStream(dataStreamHost1, dataStreamPort1);
    	final JavaReceiverInputDStream<String> inputStream2 = streamingContext.socketTextStream(dataStreamHost2, dataStreamPort2);
    	final JavaDStream<Trade> union = inputStream1.map(TradesAnalytics::convertJsonToTrade).union(inputStream2.map(TradesAnalytics::convertJsonToTrade));
    	union.foreachRDD(t -> t.collect().stream().map(t1-> t1.toString()).forEach(logger::info));
    	startStreamingAndAwaitTerminationOrTimeout(processingTimeout);
    }
    
    private static Trade convertJsonToTrade(final String jsonTrade) throws JsonParseException, JsonMappingException, IOException {
    	return mapper.readValue(jsonTrade, Trade.class);
    }
 }