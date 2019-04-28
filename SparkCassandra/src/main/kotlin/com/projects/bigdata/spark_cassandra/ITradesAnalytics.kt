package com.projects.bigdata.spark_cassandra

import com.projects.bigdata.utility.trade.Direction
import com.projects.bigdata.utility.trade.Exchange
import scala.Tuple3

interface ITradesAnalytics {
    fun averageQuantityPerSymbolDirectionExchange() : Map<Tuple3<String, Direction, Exchange>, Double>
}