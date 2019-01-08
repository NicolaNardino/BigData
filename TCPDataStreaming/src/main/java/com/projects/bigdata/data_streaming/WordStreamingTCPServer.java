package com.projects.bigdata.data_streaming;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * It sends over a socket randomly generated phrases, like the following:
 * 		word32 word783 word54 ... 
 * */
public final class WordStreamingTCPServer extends AbstractDataStreamTCPServer {
	
	public WordStreamingTCPServer(final int port, final int messageSendDelayMilliSeconds) {
		super(port, messageSendDelayMilliSeconds);
	}

	/**
	 * @return Phrases like "Word3 Word44 ...".
	 * */
	public String buildLine() {
		final int maxNrWordsPerPhrase = 100;
		final int maxNrWordPostfix = 999;
		final var phrase = IntStream.range(1, ThreadLocalRandom.current().nextInt(1, maxNrWordsPerPhrase) + 1).
		mapToObj(i -> "Word"+ThreadLocalRandom.current().nextInt(1, maxNrWordPostfix)).collect(Collectors.joining(" "));
		return phrase;
	}
	
}
