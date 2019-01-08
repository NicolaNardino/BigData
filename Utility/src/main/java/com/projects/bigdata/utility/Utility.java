package com.projects.bigdata.utility;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Utility {
	private static final Logger logger = LoggerFactory.getLogger(Utility.class);
	
	public static final List<String> Symbols = Arrays.asList("UBSN", "CSGN", "AAPL", "GOOG", "MSFT", "ACN", "AGN");
	
	public static Properties getApplicationProperties(final String propertiesFileName) throws FileNotFoundException, IOException {
		final Properties p = new Properties();
		try(final InputStream inputStream = ClassLoader.getSystemResourceAsStream(propertiesFileName)) {
			p.load(inputStream);
			return p;
		}
	}
	
	public static void shutdownExecutorService(final ExecutorService es, long timeout, TimeUnit timeUnit) throws InterruptedException {
		es.shutdown();
		if (!es.awaitTermination(timeout, timeUnit))
			es.shutdownNow();
		logger.info("ExecutorService shut down.");
	}
	
	public static void sleep(final TimeUnit timeUnit, long timeout) {
		try {
			timeUnit.sleep(timeout);
		} catch (final InterruptedException e) {
			logger.warn("Sleep interrupted.", e);
		}
	}
}
