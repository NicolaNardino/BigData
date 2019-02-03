package com.projects.bigdata.spark_streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.projects.bigdata.spark_streaming.utility.StatefulAggregationType;
import com.projects.bigdata.spark_streaming.utility.StreamingAppParameters;

public abstract class AbstractStreaming {

    final String dataStreamHost1;
    final int dataStreamPort1;
    final String dataStreamHost2;
    final int dataStreamPort2;
    final long processingTimeout;
    final StatefulAggregationType statefulAggregationType;
    final JavaStreamingContext streamingContext;

    AbstractStreaming(final StreamingAppParameters sap) {
        this.dataStreamHost1 = sap.getDataStreamHost1();
        this.dataStreamPort1 = sap.getDataStreamPort1();
        this.dataStreamHost2 = sap.getDataStreamHost2();
        this.dataStreamPort2 = sap.getDataStreamPort2();
        this.processingTimeout = sap.getProcessingTimeout();
        this.statefulAggregationType = sap.getStatefulAggregationType();
        streamingContext = new JavaStreamingContext(new SparkConf().setMaster("local["+sap.getNrThreads()+"]").setAppName("StreamingWordCount"), Durations.seconds(sap.getBatchDuration()));
        streamingContext.checkpoint(sap.getCheckpointDir());
    }

    abstract void process() throws Exception;

    void startStreamingAndAwaitTerminationOrTimeout(final long timeOutMilliSeconds) throws InterruptedException {
        streamingContext.start();
        streamingContext.awaitTerminationOrTimeout(processingTimeout);
    }
}