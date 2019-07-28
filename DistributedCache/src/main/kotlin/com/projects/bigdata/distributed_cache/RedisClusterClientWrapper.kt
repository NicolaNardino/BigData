package com.projects.bigdata.distributed_cache

import org.redisson.Redisson
import org.redisson.api.RedissonClient
import org.redisson.config.Config

class RedisClusterClientWrapper(clusterNodes: List<Pair<String, Int>>) : AutoCloseable {

    val redisson: RedissonClient

    init {
        val config = Config()
        config.useClusterServers().setScanInterval(1000).addNodeAddress(*clusterNodes.map { "redis://" + it.first + ":" + it.second }.toTypedArray())
        redisson = Redisson.create(config)
    }

    override fun close() {
        redisson.shutdown()
    }
}