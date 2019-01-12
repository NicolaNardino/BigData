package com.projects.bigdata.spark_streaming.utility;

import com.projects.bigdata.spark_streaming.AbstractStreaming;
import com.projects.bigdata.spark_streaming.StreamingWordsCount;
import com.projects.bigdata.spark_streaming.TradesAnalytics;
import com.projects.bigdata.utility.StreamingLineType;

public final class StreamingAppFactory {

	public static AbstractStreaming getStreamingApp(final StreamingLineType slt, final StreamingAppParameters sap) {
		switch(slt) {
		case PHRASE: return new StreamingWordsCount(sap);
		case TRADE: return new TradesAnalytics(sap);
		default: throw new IllegalArgumentException("Unknown applicaton type: "+slt);
		}
	}
}
