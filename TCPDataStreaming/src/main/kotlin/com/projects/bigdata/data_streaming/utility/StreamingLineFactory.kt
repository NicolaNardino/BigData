package com.projects.bigdata.data_streaming.utility

import com.projects.bigdata.utility.StreamingLineType

import java.util.HashMap
import java.util.function.Supplier

object StreamingLineFactory {
    private val streamingLineMap = mapOf<StreamingLineType, () -> String>(StreamingLineType.PHRASE to StreamingLineSupplier::randomPhrase,
            StreamingLineType.TRADE to StreamingLineSupplier::randomTrade)

    fun getStreamingLine(streamingLineType: StreamingLineType): () -> String {
        return streamingLineMap[streamingLineType]!!
    }
}
