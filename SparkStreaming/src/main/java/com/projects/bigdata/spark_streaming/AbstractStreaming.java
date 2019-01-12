package com.projects.bigdata.spark_streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.projects.bigdata.spark_streaming.utility.StreamingAppParameters;

public abstract class AbstractStreaming {
	
	final protected String dataStreamHost1; 
	final protected int dataStreamPort1;
	final protected String dataStreamHost2; 
	final protected int dataStreamPort2;
	final protected long processingTimeout;
	final protected JavaStreamingContext streamingContext;

    public AbstractStreaming(final StreamingAppParameters sap) {
    	this.dataStreamHost1 = sap.getDataStreamHost1();
    	this.dataStreamPort1 = sap.getDataStreamPort1();
    	this.dataStreamHost2 = sap.getDataStreamHost2();
    	this.dataStreamPort2 = sap.getDataStreamPort2();   	
    	this.processingTimeout = sap.getProcessingTimeout();
    	streamingContext = new JavaStreamingContext(new SparkConf().setMaster("local["+sap.getNrThreads()+"]").setAppName("StreamingWordCount"), Durations.seconds(sap.getBatchDuration()));
    	streamingContext.checkpoint(sap.getCheckpointDir());
    }
    
    public abstract void process() throws Exception;
    
    protected void startStreamingAndAwaitTerminationOrTimeout(final long timeOutMilliSeconds) throws InterruptedException {
    	streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(processingTimeout);
    }  
 }