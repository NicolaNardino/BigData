package com.projects.bigdata.spark_streaming;

import static java.util.Comparator.comparing;
import static java.util.Comparator.reverseOrder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.projects.bigdata.utility.Utility;

import scala.Tuple2;

/**
 * It solves the very basic BigData problem, i.e., the word count, although in a peculiar way. 
 * It uses Spark Streaming in order to connect to two data sources from TCP Sockets, where it gets two data streams, meant of randomly generated words.
 * It then merges the two streams and aggregates them in order to build a words ranking like the following:
 * 		(Word330,93)
 * 		(Word757,92)
 * 		(Word842,91)
 * 		(Word837,91)
 * 		(Word630,91)
 * 
 * The reason why it's compiled against Java 8 is that, unfortunately, Spark 2.4 doesn't fully support Java 10-11, for instance, collect operations fail.
 * */
public final class StreamingWordsCount {
	private static final Logger logger = LoggerFactory.getLogger(StreamingWordsCount.class);
	
	final String dataStreamHost1; 
	final int dataStreamPort1;
	final String dataStreamHost2; 
	final int dataStreamPort2;
	final int batchDuration; 
	final String checkpointDir;
	final long processingTimeout;
	final JavaStreamingContext streamingContext;

    public StreamingWordsCount(final String dataStreamHost1, final int dataStreamPort1, final String dataStreamHost2, final int dataStreamPort2, 
    		final int batchDuration, final String checkpointDir, final String nrThreads, final long processingTimeout) {
    	this.dataStreamHost1 = dataStreamHost1;
    	this.dataStreamPort1 = dataStreamPort1;
    	this.dataStreamHost2 = dataStreamHost2;
    	this.dataStreamPort2 = dataStreamPort2;
    	this.batchDuration = batchDuration;
    	this.checkpointDir = checkpointDir;
    	this.processingTimeout = processingTimeout;
    	streamingContext = new JavaStreamingContext(new SparkConf().setMaster("local["+nrThreads+"]").setAppName("StreamingWordCount"), Durations.seconds(batchDuration));
    	streamingContext.checkpoint(checkpointDir);
    }
    
    public void start() throws InterruptedException {
    	final JavaReceiverInputDStream<String> inputStream1 = streamingContext.socketTextStream(dataStreamHost1, dataStreamPort1);
    	final JavaReceiverInputDStream<String> inputStream2 = streamingContext.socketTextStream(dataStreamHost2, dataStreamPort2);
    	final JavaPairDStream<String, Integer> wordCounts = wordCountsFromStream(inputStream1).fullOuterJoin(wordCountsFromStream(inputStream2)).
    			mapToPair(f -> new Tuple2<String, Integer>(f._1, f._2._1.or(0) + f._2._2.or(0)));
    	//wordCounts.print();
    	final JavaPairDStream<String, Integer> aggregateCount = wordCounts.updateStateByKey(
                (newValues, currentValue) -> {
                	if (newValues == null || newValues.isEmpty())
                		return currentValue;
					return Optional.of(currentValue.or(0) + newValues.stream().mapToInt(Integer::valueOf).sum());
                });
    	aggregateCount.foreachRDD((counts, time) -> {
    		logger.info("Word counts aggregated at time: "+time);
    		final Comparator<Tuple2<String, Integer>> compareByCountDecr = comparing(Tuple2::_2, reverseOrder());
    		counts.collect().stream().sorted(compareByCountDecr.thenComparing(comparing(Tuple2::_1))).map(t -> t.toString()).forEach(logger::info);
		});
		
        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(processingTimeout);    
    }

    public static void main(String[] args) throws InterruptedException, FileNotFoundException, IOException {
    	System.setProperty("hadoop.home.dir", "/tmp");//otherwise: DEBUG Shell:343 - Failed to detect a valid hadoop home directory java.io.IOException: HADOOP_HOME or hadoop.home.dir are not set.
    	final Properties p = Utility.getApplicationProperties("spark_streaming.properties");
		new StreamingWordsCount(p.getProperty("streamingHost1"), Integer.valueOf(p.getProperty("streamingPort1")),
    			p.getProperty("streamingHost2"), Integer.valueOf(p.getProperty("streamingPort2")),
    			Integer.valueOf(p.getProperty("batchDurationSeconds")), p.getProperty("checkpointDir"), p.getProperty("nrThreads"), 
    			Integer.valueOf(p.getProperty("processingTimeoutMilliseconds"))).start();
    }
    
    private static JavaPairDStream<String, Integer> wordCountsFromStream(final JavaReceiverInputDStream<String> lines) {
    	return lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator())
        		.mapToPair(x -> new Tuple2<String, Integer>(x, 1))
        		.reduceByKey((x, y) -> x + y);
    }
 }