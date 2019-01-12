package com.projects.bigdata.data_streaming;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.projects.bigdata.data_streaming.utility.StreamingLineFactory;
import com.projects.bigdata.data_streaming.utility.StreamingLineSupplier;
import com.projects.bigdata.utility.StreamingLineType;
import com.projects.bigdata.utility.Utility;

public class DataStreamingTCPServersRunner {

	/**
	 * It starts two TCP Servers, waits for the Spark Streaming application to connect and then sends data.
	 * It stops after a configurable processing time.
	 * */
	public static void main(final String[] args) throws FileNotFoundException, IOException, NumberFormatException, InterruptedException {
		final var p = Utility.getApplicationProperties("server.properties");
		final var messageSendDelayMilliSeconds = Integer.valueOf(p.getProperty("messageSendDelayMilliSeconds"));	
		final var dataStreamServers = Arrays.stream(p.getProperty("port").split(",")).
		map(port -> new DataStreamingTCPServer(getStreamingLineTypeFromCommandLine(args), Integer.valueOf(port), messageSendDelayMilliSeconds)).collect(Collectors.toList());
		final var execService = Executors.newFixedThreadPool(dataStreamServers.size());
		dataStreamServers.stream().forEach(execService::execute);
		Utility.sleep(TimeUnit.SECONDS, Integer.valueOf(p.getProperty("upTimeWindowSeconds")));
		dataStreamServers.stream().forEach(DataStreamingTCPServer::stop);
		Utility.shutdownExecutorService(execService, 1, TimeUnit.SECONDS);
	}	
	
	public static Supplier<String> getStreamingLineTypeFromCommandLine(final String[] args) {
		return (args == null || args.length == 0 ? StreamingLineSupplier::randomTrade : StreamingLineFactory.getStreamingLine(StreamingLineType.valueOf(args[0])));
	}
}
