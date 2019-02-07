package com.projects.bigdata.spark_streaming

import com.projects.bigdata.spark_streaming.utility.StatefulAggregationType
import com.projects.bigdata.spark_streaming.utility.StreamingAppParameters
import org.apache.spark.api.java.Optional
import org.apache.spark.api.java.function.Function3
import org.apache.spark.streaming.State
import org.apache.spark.streaming.StateSpec
import org.apache.spark.streaming.api.java.JavaPairDStream
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream
import org.slf4j.LoggerFactory
import scala.Serializable
import scala.Tuple2
import java.util.*
import java.util.Comparator.comparing
import java.util.Comparator.reverseOrder
import java.util.function.Consumer
import java.util.function.ToIntFunction


/**
 * It solves the very basic BigData problem, i.e., the word count, although in a peculiar way.
 * It uses Spark Streaming in order to connect to two data sources from TCP Sockets, where it gets two data streams, meant of randomly generated words.
 * It then merges the two streams and aggregates them in order to build a words ranking like the following:
 * (Word330,93)
 * (Word757,92)
 * (Word630,91)
 * (Word837,91)
 * (Word842,91)
 *
 * The reason why it's compiled against Java 8 is that, unfortunately, Spark 2.4 doesn't fully support Java 10-11, for instance, collect operations fail.
 */
internal class StreamingWordsCount(sap: StreamingAppParameters) : AbstractStreaming(sap) {

    override fun process() {
        val inputStream1 = streamingContext.socketTextStream(sap.dataStreamHost1, sap.dataStreamPort1)
        val inputStream2 = streamingContext.socketTextStream(sap.dataStreamHost2, sap.dataStreamPort2)
        val wordCounts = wordCountsFromStream(inputStream1).fullOuterJoin(wordCountsFromStream(inputStream2)).mapToPair { f -> Tuple2(f._1, f._2._1.or(0) + f._2._2.or(0)) }
        when (sap.statefulAggregationType) {
            StatefulAggregationType.MapWithState -> statefulAggregationWithMapWithState(wordCounts)
            StatefulAggregationType.UpdateStateByKey -> statefulAggregationWithUpdateStateByKey(wordCounts)
        }
        startStreamingAndAwaitTerminationOrTimeout()
    }

    companion object : Serializable {
        private val logger = LoggerFactory.getLogger(StreamingWordsCount::class.java)

        private fun statefulAggregationWithMapWithState(wordCounts: JavaPairDStream<String, Int>) {
            val mappingFunc : (String, Optional<Int>, State<Int>) -> Tuple2<String, Int> = { word : String, count : Optional<Int>, state : State<Int> ->
                val sum = count.orElse(0) + if (state.exists()) state.get() else 0//sums up the occurrences of the given word.
                val result = Tuple2<String, Int>(word, sum)
                state.update(sum)
                result
            }
            with(wordCounts) {
                mapWithState<Int, Tuple2<String, Int>>(StateSpec.function(mappingFunc))
                print()
            }
        }

        private fun statefulAggregationWithUpdateStateByKey(wordCounts: JavaPairDStream<String, Int>) {
            val aggregateCount = wordCounts.updateStateByKey<Int> { newValues, currentValue ->
                if (newValues == null || newValues.isEmpty())
                    currentValue
                else
                    Optional.of(currentValue.or(0) + newValues.stream().mapToInt{ Integer.valueOf(it) }.sum())
            }
            aggregateCount.foreachRDD { counts, time ->
                logger.info("Word counts aggregated at time: $time")
                counts.collect().stream().sorted(compareBy<Tuple2<String, Int>> { it._2 }.reversed().thenComparing(compareBy<Tuple2<String, Int>> { it._1 })).
                        map { t -> t.toString() }.forEach{ logger.info(it) }
            }
        }

        /**
         * For each received line, it does the following:
         *
         *  * Splits it to words.
         *  * Maps each word to a Tuple(word, 1), so that it's ready for the subsequent aggregation step.
         *  * Aggregates/ reduces each Tuple based on its key, i.e., the word itself, by summing up each equal word.
         *
         */
        private fun wordCountsFromStream(lines: JavaReceiverInputDStream<String>): JavaPairDStream<String, Int> {
            return lines.flatMap { x -> Arrays.asList(*x.split(" ".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()).iterator() }
                    .mapToPair { x -> Tuple2(x, 1) }
                    .reduceByKey { x, y -> x!! + y!! }
        }
    }
}