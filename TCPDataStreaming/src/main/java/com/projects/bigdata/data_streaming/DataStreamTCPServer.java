package com.projects.bigdata.data_streaming;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.projects.bigdata.utility.Utility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TCP server sending randomly generated phrases. 
 * It's not meant to be a fully fledged TCP server, i.e., with a multithreaded request manager, because its only purpose is to send data to a Spark Streaming application, 
 * which is its only client.
 * 
 * The current use case consists of sending data to two ports, i.e., it opens instantiates two ServerSockets, although it could open and send data to an indefinite number of ports.  
 * */
public final class DataStreamTCPServer implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(DataStreamTCPServer.class);
	
	private final ServerSocket serverSocket;
	private final int messageSendDelayMilliSeconds;
	private final int port; 
	private volatile boolean isStopped;
	
	public DataStreamTCPServer(final int port, final int messageSendDelayMilliSeconds) {
		this.port = port;
		this.messageSendDelayMilliSeconds = messageSendDelayMilliSeconds;
		try {
			serverSocket = new ServerSocket(port);	
		}
		catch(final Exception e) {
			throw new RuntimeException("Unable to instantiate localhost server@"+port);
		}
		logger.info("Server started, localhost@"+port);
	}

	@Override
	public void run() {
		Thread.currentThread().setName("Server@"+port);
		try (final Socket clientSocket = serverSocket.accept();
			 final PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true);) {
			logger.info("Client request received.");
			while (!isStopped) {
				writer.println(buildPhrase());
				Utility.sleep(TimeUnit.MILLISECONDS, messageSendDelayMilliSeconds);
			}
		}
		catch (final Exception e) {
			logger.error("Error while sending data.", e);
		}
	}

	public void stop() {
		isStopped = true;
		try {
			serverSocket.close();
		}
		catch(final Exception e) {
			logger.error("Error while closing server socket.", e);
		}
		logger.info("Server stopped, localhost@"+port);
	}

	/**
	 * It starts two TCP Servers, waits for the Spark Streaming application to connect and then sends data.
	 * It stops after a configurable processing time.
	 * */
	public static void main(final String[] args) throws FileNotFoundException, IOException, NumberFormatException, InterruptedException {
		final var execService = Executors.newFixedThreadPool(2);
		final var properties = Utility.getApplicationProperties("server.properties");
		final var messageSendDelayMilliSeconds = Integer.valueOf(properties.getProperty("messageSendDelayMilliSeconds"));
		final var servers = Arrays.stream(properties.getProperty("port").split(",")).
		map(port -> new DataStreamTCPServer(Integer.valueOf(port), messageSendDelayMilliSeconds)).collect(Collectors.toList());
		servers.stream().forEach(execService::execute);
		Utility.sleep(TimeUnit.SECONDS, Integer.valueOf(properties.getProperty("upTimeWindowSeconds")));
		servers.stream().forEach(DataStreamTCPServer::stop);
		Utility.shutdownExecutorService(execService, 1, TimeUnit.SECONDS);
	}
	
	/**
	 * @return Phrases like "Word3 Word44 ...".
	 * */
	private static String buildPhrase() {
		final int maxNrWordsPerPhrase = 100;
		final int maxNrWordPostfix = 999;
		final var phrase = IntStream.range(1, ThreadLocalRandom.current().nextInt(1, maxNrWordsPerPhrase) + 1).
		mapToObj(i -> "Word"+ThreadLocalRandom.current().nextInt(1, maxNrWordPostfix)).collect(Collectors.joining(" "));
		logger.info("Sending line: "+phrase);
		return phrase;
	}
}
