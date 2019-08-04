package com.projects.bigdata.distributed_cache

import org.junit.jupiter.api.Test
import org.redisson.api.RedissonClient
import java.time.LocalDate
import java.util.ArrayList
import kotlin.test.assertEquals

/**
 * This test class is to check the connectivity with a Redis Cluster and do some data sharding operations.
 * Cluster set up on localhost: 3 Masters (7000, 7001, 7002), 3 Slaves (7001, 7002, 7003)
 * */
class RedisClusterClientWrapperTest {
    private val clusterMasterNodes: List<Pair<String, Int>> = listOf(Pair("127.0.0.1", 7000), Pair("127.0.0.1", 7001), Pair("127.0.0.1", 7002))

    /**
     * https://redis.io/topics/cluster-spec#keys-hash-tags
     * Multi-key operations on a Redis-Cluster have to be sharded to the same slot.
     * */
    @Test
    fun redisClusterBatchBucketsTest() {

        RedisClusterClientWrapper(clusterMasterNodes).use {
            batchBucketsSupport(it.redisson, timeSeriesNamesHashTagged, timeSeriesParams)
        }
    }

    /**
     * Keys are evenly sharded by the hash-based Redis algorithm.
     * */
    @Test
    fun redisClusterBucketsTest() {
        RedisClusterClientWrapper(clusterMasterNodes).use {
            with(it.redisson) {
                timeSeriesNames.forEach{ it1 ->
                    val timeSeriesBucket = getBucket<TimeSeriesItem>(it1)
                    val timeSeriesItem = TimeSeriesItem(mapOf("A" to random.nextDouble()), LocalDate.of(2015, 10, 12), random.nextDouble())
                    timeSeriesBucket.set(timeSeriesItem)
                    assertEquals(timeSeriesBucket.get(), timeSeriesItem)
                }
                printKeySlots(it.redisson, timeSeriesNames)
            }
        }
    }

    fun printKeySlots(redisson: RedissonClient, keys: Set<String>) {
        keys.forEach { println("$it -> ${redisson.keys.getSlot(it)}") }
    }
}