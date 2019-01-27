package com.projects.bigdata.spark_streaming;

import java.util.Properties;

import com.projects.bigdata.spark_streaming.utility.StatefulAggregationType;
import com.projects.bigdata.spark_streaming.utility.StreamingAppFactory;
import com.projects.bigdata.spark_streaming.utility.StreamingAppParameters;
import com.projects.bigdata.utility.StreamingLineType;

import com.projects.bigdata.utility.Utility;

public final class StreamingRunner {

    public static void main(final String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "/tmp");//otherwise: DEBUG Shell:343 - Failed to detect a valid Hadoop home directory java.io.IOException: HADOOP_HOME or hadoop.home.dir are not set.
        final Properties p = Utility.getApplicationProperties("spark_streaming.properties");
        final StreamingAppParameters sap = new StreamingAppParameters(p.getProperty("streamingHost1"), Integer.valueOf(p.getProperty("streamingPort1")),
                p.getProperty("streamingHost2"), Integer.valueOf(p.getProperty("streamingPort2")),
                Integer.valueOf(p.getProperty("batchDurationSeconds")), p.getProperty("checkpointDir"), p.getProperty("nrThreads"),
                Integer.valueOf(p.getProperty("processingTimeoutMilliseconds")), StatefulAggregationType.MapWithState);
        getStreamingApp(args, sap).process();
    }

    private static AbstractStreaming getStreamingApp(final String[] args, final StreamingAppParameters sap) {
        return (args == null || args.length == 0 ? new TradesAnalytics(sap) :
                StreamingAppFactory.getStreamingApp(StreamingLineType.valueOf(args[0]), sap));
    }
}
