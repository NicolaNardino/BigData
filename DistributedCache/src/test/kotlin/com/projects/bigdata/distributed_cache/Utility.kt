package com.projects.bigdata.distributed_cache

import org.redisson.api.RedissonClient
import java.time.LocalDate
import java.util.*
import java.util.stream.Collectors
import kotlin.test.assertEquals

val timeSeriesNames = setOf("N1", "N2", "N3", "N4")
val timeSeriesNamesHashTagged = setOf("{mykey}.N1", "{mykey}.N2", "{mykey}.N3", "{mykey}.N4", "{mykey}.N5", "{mykey}.N6", "{mykey}.N7")
val timeSeriesParams = setOf("P1", "P2", "P3", "P4", "P5", "P6", "P7")
val random = Random()

fun batchBucketsSupport(redissonClient: RedissonClient, timeSeriesNames: Set<String>, timeSeriesParams: Set<String>) {
    val timeSeriesNamesArray: Array<String?> = arrayOfNulls(timeSeriesNames.size)
    (timeSeriesNames.toList() as ArrayList<String>).toArray(timeSeriesNamesArray)
    val inputMap: MutableMap<String, List<TimeSeriesItem>> = timeSeriesNames.stream().collect(Collectors.toMap({ k -> k }) { buildTimeSeries(timeSeriesParams, LocalDate.of(2015, 1, 1), LocalDate.now()) })
    redissonClient.buckets.set(inputMap)
    val resultFromRedisMap : Map<String, List<TimeSeriesItem>> = redissonClient.buckets.get(*timeSeriesNamesArray)
    assertEquals(resultFromRedisMap.keys, inputMap.keys)
}

