package com.projects.bigdata.data_streaming

import com.projects.bigdata.utility.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.io.PrintWriter
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.TimeUnit

/**
 * TCP server sending randomly generated strings, be it random words or JSON representations of Objects.
 * It's not meant to be a fully fledged TCP server, i.e., with a multithreaded request manager, because its only purpose is to send data to a Spark Streaming application,
 * which is its only client.
 *
 * The current use case consists of sending data to two ports. It instantiates two ServerSockets, although it could open and send data to an indefinite number of ports.
 */
class DataStreamingTCPServer(private val streamingLineBuilder: () -> String, private val port: Int, private val messageSendDelayMilliSeconds: Int) : Runnable {

    private val serverSocket: ServerSocket
    @Volatile
    private var isStopped: Boolean = false

    init {
        try {
            serverSocket = ServerSocket(port)
        } catch (e: Exception) {
            throw RuntimeException("Unable to instantiate localhost server@$port", e)
        }

        logger.info("Server started, localhost@$port")
    }

    override fun run() {
        Thread.currentThread().name = "Server@$port"
        try {
            serverSocket.accept().use { clientSocket ->
                PrintWriter(clientSocket.getOutputStream(), true).use { writer ->
                    logger.info("Client request received.")
                    while (!isStopped) {
                        val line = streamingLineBuilder.invoke()
                        logger.info("Sending line: $line")
                        writer.println(line)
                        sleep(TimeUnit.MILLISECONDS, messageSendDelayMilliSeconds.toLong())
                    }
                }
            }
        } catch (e: Exception) {
            logger.error("Error while sending data.", e)
        }

    }

    fun stop() {
        isStopped = true
        try {
            serverSocket.close()
        } catch (e: Exception) {
            logger.error("Error while closing server socket.", e)
        }

        logger.info("Server stopped, localhost@$port")
    }

    companion object {
        private val logger = LoggerFactory.getLogger(DataStreamingTCPServer::class.java)
    }
}
