package com.projects.bigdata.distributed_cache

import java.io.Serializable
import java.time.LocalDate

data class TimeSeriesItem(val params: Map<String, Double>, val date: LocalDate, val value: Double) : Serializable {

    companion object {
        private const val serialVersionUID = 1L
    }
}