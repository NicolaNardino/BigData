package com.projects.bigdata.spark_streaming.utility

import com.projects.bigdata.spark_streaming.*
import com.projects.bigdata.utility.StreamingLineType

object StreamingAppFactory {

    fun getStreamingApp(slt: StreamingLineType, sap: StreamingAppParameters): AbstractStreaming = when (slt) {
            StreamingLineType.PHRASE -> StreamingWordsCount(sap)
            StreamingLineType.TRADE -> TradesAnalytics(sap)
            else -> throw IllegalArgumentException("Unknown application type: $slt")
    }
}
