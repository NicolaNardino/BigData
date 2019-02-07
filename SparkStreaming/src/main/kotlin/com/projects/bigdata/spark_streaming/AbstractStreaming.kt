package com.projects.bigdata.spark_streaming

import com.projects.bigdata.spark_streaming.utility.StreamingAppParameters
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.api.java.JavaStreamingContext

abstract class AbstractStreaming(val sap: StreamingAppParameters) {

    internal val streamingContext: JavaStreamingContext = JavaStreamingContext(SparkConf().setMaster("local[" + sap.nrThreads + "]").setAppName("StreamingWordCount"), Durations.seconds(sap.batchDuration.toLong()))

    init {
        streamingContext.checkpoint(sap.checkpointDir)
    }

    internal abstract fun process()

    internal fun startStreamingAndAwaitTerminationOrTimeout() {
        with (streamingContext) {
            start()
            awaitTerminationOrTimeout(sap.processingTimeout)
        }
    }
}