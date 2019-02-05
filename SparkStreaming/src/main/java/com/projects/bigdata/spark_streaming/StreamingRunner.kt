package com.projects.bigdata.spark_streaming

import com.projects.bigdata.spark_streaming.utility.StatefulAggregationType
import com.projects.bigdata.spark_streaming.utility.StreamingAppFactory
import com.projects.bigdata.spark_streaming.utility.StreamingAppParameters
import com.projects.bigdata.utility.StreamingLineType
import com.projects.bigdata.utility.getApplicationProperties

fun main(args: Array<String>) {
    fun getStreamingApp(args: Array<String>, sap: StreamingAppParameters): AbstractStreaming {
        return if (args.isEmpty())
            TradesAnalytics(sap)
        else
            StreamingAppFactory.getStreamingApp(StreamingLineType.valueOf(args[0]), sap)
    }
    System.setProperty("hadoop.home.dir", "/tmp")//otherwise: DEBUG Shell:343 - Failed to detect a valid Hadoop home directory java.io.IOException: HADOOP_HOME or hadoop.home.dir are not set.
    val p = getApplicationProperties("spark_streaming.properties")
    val sap = StreamingAppParameters(p.getProperty("streamingHost1"), Integer.valueOf(p.getProperty("streamingPort1")),
            p.getProperty("streamingHost2"), Integer.valueOf(p.getProperty("streamingPort2")),
            Integer.valueOf(p.getProperty("batchDurationSeconds")), p.getProperty("checkpointDir"), p.getProperty("nrThreads"),
            Integer.valueOf(p.getProperty("processingTimeoutMilliseconds")).toLong(), StatefulAggregationType.MapWithState)
    getStreamingApp(args, sap).process()
}