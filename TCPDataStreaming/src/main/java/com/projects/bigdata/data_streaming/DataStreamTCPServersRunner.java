package com.projects.bigdata.data_streaming;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.projects.bigdata.utility.Utility;

public class DataStreamTCPServersRunner {

	/**
	 * It starts two TCP Servers, waits for the Spark Streaming application to connect and then sends data.
	 * It stops after a configurable processing time.
	 * */
	public static void main(final String[] args) throws FileNotFoundException, IOException, NumberFormatException, InterruptedException {
		final var properties = Utility.getApplicationProperties("server.properties");
		final var messageSendDelayMilliSeconds = Integer.valueOf(properties.getProperty("messageSendDelayMilliSeconds"));
		final var dataStreamServers = Arrays.stream(properties.getProperty("port").split(",")).
		map(port -> new WordStreamingTCPServer(Integer.valueOf(port), messageSendDelayMilliSeconds)).collect(Collectors.toList());
		final var execService = Executors.newFixedThreadPool(dataStreamServers.size());
		dataStreamServers.stream().forEach(execService::execute);
		Utility.sleep(TimeUnit.SECONDS, Integer.valueOf(properties.getProperty("upTimeWindowSeconds")));
		dataStreamServers.stream().forEach(AbstractDataStreamTCPServer::stop);
		Utility.shutdownExecutorService(execService, 1, TimeUnit.SECONDS);
	}

}
