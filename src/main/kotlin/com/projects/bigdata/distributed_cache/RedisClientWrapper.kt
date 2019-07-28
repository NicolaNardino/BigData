package com.projects.bigdata.distributed_cache

import org.redisson.Redisson
import org.redisson.api.RedissonClient
import org.redisson.config.Config

class RedisClientWrapper(host: String, port: Int) : AutoCloseable {

    val redisson: RedissonClient

    init {
        val config = Config()
        config.useSingleServer().address = "redis://$host:$port"
        redisson = Redisson.create(config)
    }

    override fun close() {
        redisson.shutdown()
    }
}