package com.projects.bigdata.distributed_cache

import org.junit.jupiter.api.Test
import org.redisson.api.RMap
import java.time.LocalDate
import java.util.stream.StreamSupport
import kotlin.test.assertEquals
import kotlin.test.assertTrue


class RedisClientWrapperTest {
    private val redisConnectionParams = Pair("localhost", 6379)

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
            batchBucketsSupport(it.redisson, timeSeriesNames, timeSeriesParams)
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