package com.projects.bigdata.cassandra_microservice.error

import java.util.Date

class RequestErrorWrapper(val errorDate: Date, val message: String, val details: String)