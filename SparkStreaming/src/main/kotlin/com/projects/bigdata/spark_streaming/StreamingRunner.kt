@file:JvmName("StreamingRunner")

package com.projects.bigdata.spark_streaming

import com.projects.bigdata.spark_streaming.utility.StatefulAggregationType
import com.projects.bigdata.spark_streaming.utility.StreamingAppFactory
import com.projects.bigdata.spark_streaming.utility.StreamingAppParameters
import com.projects.bigdata.utility.StreamingLineType
import com.projects.bigdata.utility.getApplicationProperties

fun main(args: Array<String>) {
    fun getStreamingApp(args: Array<String>, sap: StreamingAppParameters): AbstractStreaming = if (args.isEmpty())
        TradesAnalytics(sap)
    else
        StreamingAppFactory.getStreamingApp(StreamingLineType.valueOf(args[0]), sap)

    System.setProperty("hadoop.home.dir", "/tmp")//otherwise: DEBUG Shell:343 - Failed to detect a valid Hadoop home directory java.io.IOException: HADOOP_HOME or hadoop.home.dir are not set.
    with (getApplicationProperties("spark_streaming.properties")) {
        getStreamingApp(args, StreamingAppParameters(getProperty("streamingHost1"), Integer.valueOf(getProperty("streamingPort1")),
                getProperty("streamingHost2"), Integer.valueOf(getProperty("streamingPort2")),
                Integer.valueOf(getProperty("batchDurationSeconds")), getProperty("checkpointDir"), getProperty("nrThreads"),
                Integer.valueOf(getProperty("processingTimeoutMilliseconds")).toLong(), StatefulAggregationType.MapWithState)).process()
    }
}