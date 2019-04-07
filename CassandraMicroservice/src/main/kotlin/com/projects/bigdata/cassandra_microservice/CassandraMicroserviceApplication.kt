package com.projects.bigdata.cassandra_microservice

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication

@SpringBootApplication
class CassandraMicroserviceApplication
    fun main(args: Array<String>) {
        SpringApplication.run(CassandraMicroserviceApplication::class.java, *args)
    }