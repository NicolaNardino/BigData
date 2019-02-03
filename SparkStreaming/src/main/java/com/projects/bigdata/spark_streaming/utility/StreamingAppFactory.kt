package com.projects.bigdata.spark_streaming.utility

import com.projects.bigdata.spark_streaming.AbstractStreaming
import com.projects.bigdata.spark_streaming.StreamingWordsCount
import com.projects.bigdata.spark_streaming.TradesAnalytics
import com.projects.bigdata.utility.StreamingLineType

object StreamingAppFactory {

    fun getStreamingApp(slt: StreamingLineType, sap: StreamingAppParameters): AbstractStreaming = when (slt) {
            StreamingLineType.PHRASE -> StreamingWordsCount(sap)
            StreamingLineType.TRADE -> TradesAnalytics(sap)
            else -> throw IllegalArgumentException("Unknown application type: $slt")
    }
}
