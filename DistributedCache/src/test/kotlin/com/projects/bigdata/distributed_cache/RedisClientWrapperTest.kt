package com.projects.bigdata.distributed_cache

import org.junit.jupiter.api.Test
import org.redisson.api.RMap
import java.time.LocalDate
import kotlin.test.assertEquals

class RedisClientWrapperTest {
    private val redisConnectionParams = Pair("localhost", 6379)

    @Test
    fun redisMapTest() {
        val timeSeriesMapName = "TimeSeriesMap"
        val timeSeriesNames = setOf("N1", "N2", "N3", "N4")
        val timeSeriesParams = setOf("P1", "P2", "P3", "P4", "P5", "P6", "P7")
        RedisClientWrapper(redisConnectionParams.first, redisConnectionParams.second).use{
            val timeSeriesMap: RMap<String, List<TimeSeriesItem>> = it.redisson.getMap(timeSeriesMapName)
            timeSeriesMap.delete()
            timeSeriesNames.forEach{ p -> timeSeriesMap[p] = buildTimeSeries(timeSeriesParams, LocalDate.of(2009,1,1), LocalDate.now()) }
            assertEquals(timeSeriesMap.keys, timeSeriesNames)
            println(timeSeriesMap.entries.forEach{ es -> println("${es.key}/ ${es.value.size}\n")})
        }
    }
}