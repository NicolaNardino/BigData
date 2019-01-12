package com.projects.bigdata.data_streaming.utility;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import com.projects.bigdata.utility.StreamingLineType;

public final class StreamingLineFactory {
	private static final Map<StreamingLineType, Supplier<String>> streamingLineMap = new HashMap<>() {
		private static final long serialVersionUID = 1L;

	{
		put(StreamingLineType.PHRASE, StreamingLineSupplier::randomPhrase);
		put(StreamingLineType.TRADE, StreamingLineSupplier::randomTrade);
	}};
	
	public static Supplier<String> getStreamingLine(final StreamingLineType streaminLineType)  {
		return streamingLineMap.get(streaminLineType);
	}
}
