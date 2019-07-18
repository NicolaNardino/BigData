@file:JvmName("TimeSeriesBuilder")

package com.projects.bigdata.distributed_cache

import java.time.LocalDate
import java.util.*
import java.util.stream.IntStream
import java.time.temporal.ChronoUnit
import java.util.stream.Collectors.toList
import java.util.stream.Collectors.toMap
import java.util.stream.Stream

val random: Random = Random()

fun buildTimeSeries(params: Set<String>, startDate: LocalDate, endDate: LocalDate): List<TimeSeriesItem> =
        getDatesBetween(startDate, endDate).map { ld -> TimeSeriesItem(params.stream().collect(toMap({ it }) { random.nextDouble() }), ld, random.nextDouble()) }.collect(toList())

private fun getDatesBetween(startDate: LocalDate, endDate: LocalDate): Stream<LocalDate> =
        IntStream.iterate(0) { it + 1 }.limit(ChronoUnit.DAYS.between(startDate, endDate)).mapToObj { startDate.plusDays(it.toLong()) }





