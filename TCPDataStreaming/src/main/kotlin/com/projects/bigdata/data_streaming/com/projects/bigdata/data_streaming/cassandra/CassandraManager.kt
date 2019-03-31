package com.projects.bigdata.data_streaming.cassandra

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session
import org.slf4j.LoggerFactory

class CassandraManager(node: String, port: Int) : AutoCloseable {

    private val cluster:  Cluster = Cluster.builder().addContactPoint(node).withPort(port).build()
    val session: Session

    init {
        session = cluster.connect()
        logger.info("Cassandra session connected, $node@$port")
    }

    override fun close() {
        session.close()
        cluster.close()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(CassandraManager::class.java)
    }

}