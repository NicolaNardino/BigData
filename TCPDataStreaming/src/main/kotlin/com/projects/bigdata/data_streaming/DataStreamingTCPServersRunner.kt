package com.projects.bigdata.data_streaming

import com.projects.bigdata.data_streaming.utility.StreamingLineFactory
import com.projects.bigdata.data_streaming.utility.StreamingLineSupplier
import com.projects.bigdata.utility.StreamingLineType
import com.projects.bigdata.utility.*

import java.io.FileNotFoundException
import java.io.IOException
import java.util.Arrays
import java.util.Properties
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.function.Consumer
import java.util.stream.Collectors

/**
 * It starts two TCP Servers, waits for the Spark Streaming application to connect and then sends data.
 * It stops after a configurable processing time.
 */
fun main(args: Array<String>) {
    fun getStreamingLineTypeFromCommandLine(args: Array<String>?): () -> String {
        return if (args != null && !args.isEmpty())
            StreamingLineFactory.getStreamingLine(StreamingLineType.valueOf(args[0]))
        else
            StreamingLineSupplier::randomTrade
    }
    val p = getApplicationProperties("server.properties")
    val messageSendDelayMilliSeconds = Integer.valueOf(p.getProperty("messageSendDelayMilliSeconds"))
    val dataStreamServers = Arrays.stream(p.getProperty("port").split(",".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()).
            map { port -> DataStreamingTCPServer(getStreamingLineTypeFromCommandLine(args), Integer.valueOf(port), messageSendDelayMilliSeconds) }.
            collect(Collectors.toList())
    val execService = Executors.newFixedThreadPool(dataStreamServers.size)
    dataStreamServers.asSequence().forEach { execService.execute(it) }
    sleep(TimeUnit.SECONDS, Integer.valueOf(p.getProperty("upTimeWindowSeconds")).toLong())
    dataStreamServers.stream().forEach { it.stop() }
    shutdownExecutorService(execService, 1, TimeUnit.SECONDS)
}