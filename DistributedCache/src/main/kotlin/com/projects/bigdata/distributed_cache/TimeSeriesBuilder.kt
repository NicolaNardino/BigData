@file:JvmName("TimeSeriesBuilder")

package com.projects.bigdata.distributed_cache

import java.time.LocalDate
import java.util.*
import java.util.stream.IntStream
import java.time.temporal.ChronoUnit
import java.util.stream.Collectors.toSet
import java.util.stream.Collectors.toMap
import java.util.stream.Stream

val random: Random = Random()

val timeSeriesNames = setOf("N1", "N2", "N3", "N4")
val timeSeriesNamesHashTagged = setOf("{mykey}.N1", "{mykey}.N2", "{mykey}.N3", "{mykey}.N4", "{mykey}.N5", "{mykey}.N6", "{mykey}.N7")
val timeSeriesParams = setOf("P1", "P2", "P3", "P4", "P5", "P6", "P7")

fun buildTimeSeries(params: Set<String>, startDate: LocalDate, endDate: LocalDate): Set<TimeSeriesItem> =
        getDatesBetween(startDate, endDate).map { ld -> TimeSeriesItem(params.stream().collect(toMap({ it }) { random.nextDouble() }), ld, random.nextDouble()) }.collect(toSet())

private fun getDatesBetween(startDate: LocalDate, endDate: LocalDate): Stream<LocalDate> =
        IntStream.iterate(0) { it + 1 }.limit(ChronoUnit.DAYS.between(startDate, endDate)).mapToObj { startDate.plusDays(it.toLong()) }





