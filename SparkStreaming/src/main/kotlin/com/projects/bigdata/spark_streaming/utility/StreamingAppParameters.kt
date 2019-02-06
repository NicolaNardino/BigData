package com.projects.bigdata.spark_streaming.utility

data class StreamingAppParameters(val dataStreamHost1: String, val dataStreamPort1: Int, val dataStreamHost2: String,
                             val dataStreamPort2: Int, val batchDuration: Int, val checkpointDir: String, val nrThreads: String, val processingTimeout: Long, val statefulAggregationType: StatefulAggregationType)