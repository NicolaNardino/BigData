@file:JvmName("Utility")

package com.projects.bigdata.utility

import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit

val Symbols: List<String> = listOf("UBSN", "CSGN", "AAPL", "GOOG", "MSFT", "ACN", "AGN")

private val logger = LoggerFactory.getLogger("com.projects.bigdata.utility.Utility")

fun shutdownExecutorService(es: ExecutorService, timeout: Long, timeUnit: TimeUnit) {
    es.shutdown()
    if (!es.awaitTermination(timeout, timeUnit))
        es.shutdownNow()
    logger.info("ExecutorService shut down.")
}

fun getApplicationProperties(propertiesFileName: String): Properties =
        ClassLoader.getSystemResourceAsStream(propertiesFileName)!!.use {
            val p = Properties()
            p.load(it)
            return p
        }

fun sleep(timeUnit: TimeUnit, timeout: Long) = try {
    timeUnit.sleep(timeout)
} catch (e: InterruptedException) {
    logger.warn("Sleep interrupted.", e)
}