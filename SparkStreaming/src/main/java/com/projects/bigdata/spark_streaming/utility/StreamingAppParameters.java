package com.projects.bigdata.spark_streaming.utility;

public final class StreamingAppParameters {
	private final String dataStreamHost1; 
	private final int dataStreamPort1; 
	private final String dataStreamHost2; 
	private final int dataStreamPort2; 
	private final int batchDuration; 
	private final String checkpointDir; 
	private final String nrThreads; 
	private final long processingTimeout;
	
	public StreamingAppParameters(final String dataStreamHost1, final int dataStreamPort1, final String dataStreamHost2,
			final int dataStreamPort2, final int batchDuration, final String checkpointDir, final String nrThreads, final long processingTimeout) {
		this.dataStreamHost1 = dataStreamHost1;
		this.dataStreamPort1 = dataStreamPort1;
		this.dataStreamHost2 = dataStreamHost2;
		this.dataStreamPort2 = dataStreamPort2;
		this.batchDuration = batchDuration;
		this.checkpointDir = checkpointDir;
		this.nrThreads = nrThreads;
		this.processingTimeout = processingTimeout;
	}

	public String getDataStreamHost1() {
		return dataStreamHost1;
	}

	public int getDataStreamPort1() {
		return dataStreamPort1;
	}

	public String getDataStreamHost2() {
		return dataStreamHost2;
	}

	public int getDataStreamPort2() {
		return dataStreamPort2;
	}

	public int getBatchDuration() {
		return batchDuration;
	}

	public String getCheckpointDir() {
		return checkpointDir;
	}

	public String getNrThreads() {
		return nrThreads;
	}

	public long getProcessingTimeout() {
		return processingTimeout;
	}
}
