package com.projects.bigdata.distributed_cache

import org.junit.jupiter.api.Test
import org.redisson.api.RMap
import java.time.LocalDate
import java.util.stream.Collectors
import java.util.stream.StreamSupport
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import java.util.*


class RedisClientWrapperTest {
    private val redisConnectionParams = Pair("localhost", 6379)
    private val timeSeriesNames = setOf("N1", "N2", "N3", "N4")
    private val timeSeriesParams = setOf("P1", "P2", "P3", "P4", "P5", "P6", "P7")

    @Test
    fun redisMapTest() {
        val timeSeriesMapName = "TimeSeriesMap"
        RedisClientWrapper(redisConnectionParams.first, redisConnectionParams.second).use{
            with(it.redisson) {
                val timeSeriesMap: RMap<String, List<TimeSeriesItem>> = getMap(timeSeriesMapName)
                timeSeriesMap.delete()
                timeSeriesNames.forEach{ p -> timeSeriesMap[p] = buildTimeSeries(timeSeriesParams, LocalDate.of(2009,1,1), LocalDate.now()) }
                assertEquals(timeSeriesMap.keys, timeSeriesNames)
                assertTrue { StreamSupport.stream(keys.getKeysByPattern(timeSeriesMapName).spliterator(), false).filter{t -> t == timeSeriesMapName }.findAny().isPresent }
                //println(timeSeriesMap.entries.forEach{ es -> println("${es.key}/ ${es.value.size}\n")})
            }
        }
    }

    @Test
    fun redisBatchBucketsTest() {
        RedisClientWrapper(redisConnectionParams.first, redisConnectionParams.second).use{
            with(it.redisson) {
                val timeSeriesNamesArray: Array<String?> = arrayOfNulls(timeSeriesNames.size)
                (timeSeriesNames.toList() as ArrayList<String>).toArray(timeSeriesNamesArray)
                val inputMap: MutableMap<String, List<TimeSeriesItem>> = timeSeriesNames.stream().collect(Collectors.toMap({ k -> k }) { buildTimeSeries(timeSeriesParams, LocalDate.of(2015, 1, 1), LocalDate.now()) })
                buckets.set(inputMap)
                val resultFromRedisMap : Map<String, List<TimeSeriesItem>> = buckets.get(*timeSeriesNamesArray)
                assertEquals(resultFromRedisMap.keys, inputMap.keys)
            }
        }
    }

    @Test
    fun redisBucketAsyncTest() {
        RedisClientWrapper(redisConnectionParams.first, redisConnectionParams.second).use{
            with(it.redisson) {
                val timeSeriesBucket = getBucket<TimeSeriesItem>("timeSeriesItem")
                val timeSeriesItem = TimeSeriesItem(mapOf("A" to 10.1), LocalDate.of(2015, 10, 12), Double.MIN_VALUE)
                timeSeriesBucket.setAsync(timeSeriesItem).whenComplete{ _, _ -> assertEquals(timeSeriesItem, timeSeriesBucket.get()) }
            }
        }
    }
}