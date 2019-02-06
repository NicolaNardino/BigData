package com.projects.bigdata.spark_streaming

import com.projects.bigdata.spark_streaming.utility.StreamingAppParameters
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.api.java.JavaStreamingContext

abstract class AbstractStreaming(val sap: StreamingAppParameters) {

    val streamingContext: JavaStreamingContext = JavaStreamingContext(SparkConf().setMaster("local[" + sap.nrThreads + "]").setAppName("StreamingWordCount"), Durations.seconds(sap.batchDuration.toLong()))

    init {
        streamingContext.checkpoint(sap.checkpointDir)
    }

    abstract fun process()

    fun startStreamingAndAwaitTerminationOrTimeout() {
        streamingContext.start()
        streamingContext.awaitTerminationOrTimeout(sap.processingTimeout)
    }
}