package com.projects.bigdata.spark_streaming;

import static java.util.Comparator.comparing;
import static java.util.Comparator.reverseOrder;

import java.util.Arrays;
import java.util.Comparator;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.projects.bigdata.spark_streaming.utility.StreamingAppParameters;

import scala.Tuple2;


/**
 * It solves the very basic BigData problem, i.e., the word count, although in a peculiar way.
 * It uses Spark Streaming in order to connect to two data sources from TCP Sockets, where it gets two data streams, meant of randomly generated words.
 * It then merges the two streams and aggregates them in order to build a words ranking like the following:
 * 		(Word330,93)
 * 		(Word757,92)
 * 		(Word630,91)
 * 		(Word837,91)
 * 		(Word842,91)
 *
 * The reason why it's compiled against Java 8 is that, unfortunately, Spark 2.4 doesn't fully support Java 10-11, for instance, collect operations fail.
 * */
public final class StreamingWordsCount extends AbstractStreaming {
    private static final Logger logger = LoggerFactory.getLogger(StreamingWordsCount.class);

    public StreamingWordsCount(final StreamingAppParameters sap) {
        super(sap);
    }

    public void process() throws InterruptedException {
        final JavaReceiverInputDStream<String> inputStream1 = streamingContext.socketTextStream(dataStreamHost1, dataStreamPort1);
        final JavaReceiverInputDStream<String> inputStream2 = streamingContext.socketTextStream(dataStreamHost2, dataStreamPort2);
        final JavaPairDStream<String, Integer> wordCounts = wordCountsFromStream(inputStream1).fullOuterJoin(wordCountsFromStream(inputStream2)).
                mapToPair(f -> new Tuple2<>(f._1, f._2._1.or(0) + f._2._2.or(0)));
        switch(statefulAggregationType) {
            case MapWithState: statefulAggregationWithMapWithState(wordCounts); break;
            case UpdateStateByKey: statefulAggregationWithUpdateStateByKey(wordCounts); break;
        }
        startStreamingAndAwaitTerminationOrTimeout(processingTimeout);
    }

    private static void statefulAggregationWithMapWithState(final JavaPairDStream<String, Integer> wordCounts) {
        final Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mappingFunc = (word, count, state) -> {
            final int sum = count.orElse(0) + (state.exists() ? state.get() : 0);//sums up the occurrences of the given word.
            final Tuple2<String, Integer> result = new Tuple2<>(word, sum);
            state.update(sum);
            return result;
        };
        wordCounts.mapWithState(StateSpec.function(mappingFunc));
        wordCounts.print();
    }

    private static void statefulAggregationWithUpdateStateByKey(final JavaPairDStream<String, Integer> wordCounts) {
        final JavaPairDStream<String, Integer> aggregateCount = wordCounts.updateStateByKey(
                (newValues, currentValue) -> {
                    if (newValues == null || newValues.isEmpty())
                        return currentValue;
                    return Optional.of(currentValue.or(0) + newValues.stream().mapToInt(Integer::valueOf).sum());
                });
        aggregateCount.foreachRDD((counts, time) -> {
            logger.info("Word counts aggregated at time: "+time);
            final Comparator<Tuple2<String, Integer>> compareByCountDecr = comparing(Tuple2::_2, reverseOrder());
            counts.collect().stream().sorted(compareByCountDecr.thenComparing(comparing(Tuple2::_1))).map(t -> t.toString()).forEach(logger::info);
        });
    }

    /**
     * For each received line, it does the following:
     * <ul>
     * 	<li>Splits it to words.</li>
     * 	<li>Maps each word to a Tuple(word, 1), so that it's ready for the subsequent aggregation step.</li>
     * 	<li>Aggregates/ reduces each Tuple based on its key, i.e., the word itself, by summing up each equal word.</li>
     * </ul>
     * */
    private static JavaPairDStream<String, Integer> wordCountsFromStream(final JavaReceiverInputDStream<String> lines) {
        return lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator())
                .mapToPair(x -> new Tuple2<>(x, 1))
                .reduceByKey((x, y) -> x + y);
    }
}