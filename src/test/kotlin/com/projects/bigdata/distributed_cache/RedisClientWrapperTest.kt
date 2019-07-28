package com.projects.bigdata.distributed_cache

import org.junit.jupiter.api.Test
import org.redisson.api.RMap
import java.time.LocalDate
import kotlin.test.assertEquals

class RedisClientWrapperTest {
    private val redisConnectionParams = Pair("localhost", 6379)


    @Test
    fun redisTest() {
        val timeSeriesMapName = "TimeSeriesMap"
        val timeSeriesNames = setOf("N1", "N2", "N3")
        val timeSeriesParams = setOf("P1", "P2", "P3", "P4")
        RedisClientWrapper(redisConnectionParams.first, redisConnectionParams.second).use{
            val map: RMap<String, List<TimeSeriesItem>> = it.redisson.getMap(timeSeriesMapName)
            if (map.size == 0)
                timeSeriesNames.forEach{ p -> map[p] = buildTimeSeries(timeSeriesParams, LocalDate.of(2010,1,1), LocalDate.now()) }
        }
        RedisClientWrapper(redisConnectionParams.first, redisConnectionParams.second).use{
            val map: RMap<String, List<TimeSeriesItem>> = it.redisson.getMap(timeSeriesMapName)
            assertEquals(map.keys, timeSeriesNames)
        }
    }
}