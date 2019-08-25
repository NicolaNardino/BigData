package com.projects.bigdata.distributed_cache

import org.redisson.api.RedissonClient
import java.time.LocalDate
import java.util.*
import java.util.stream.Collectors
import kotlin.test.assertEquals


val random = Random()

fun batchBucketsSupport(redissonClient: RedissonClient, timeSeriesNames: Set<String>, timeSeriesParams: Set<String>) {
    val timeSeriesNamesArray: Array<String?> = arrayOfNulls(timeSeriesNames.size)
    (timeSeriesNames.toList() as ArrayList<String>).toArray(timeSeriesNamesArray)
    val inputMap: MutableMap<String, Set<TimeSeriesItem>> = timeSeriesNames.stream().collect(Collectors.toMap({ k -> k }) { buildTimeSeries(timeSeriesParams, LocalDate.of(2015, 1, 1), LocalDate.now()) })
    redissonClient.buckets.set(inputMap)
    val resultFromRedisMap : Map<String, List<TimeSeriesItem>> = redissonClient.buckets.get(*timeSeriesNamesArray)
    assertEquals(resultFromRedisMap.keys, inputMap.keys)
}

