# Welcome to BigData.Spark

It's about various BigData problems solved with Spark and, in future, NoSQL technologies. 

## Word count and ranking of multiple data streams
The first one of the most recurrent BigData problems: the word count and ranking.
Set up:
- A Spark Streaming application (SSA) connects to two TCP servers.
- Once the connection is established, the TCP servers start sending data, in the form of randomly generated phrases.
- The SSA aggregates the data by (full-outer) joining the two input streams and finally builds a ranking. 

The beauty of this is that the SSA can continuously receive and apply transformation to the received data, grouping it in pre-defined time frames (batch duration).

## Trades Analytics
The data streamers send JSON representations of Trades (symbol, price, qty, direction (buy/ sell), exchange), the SSA decodes them into Trades object and finally applies stateful transformations based on JavaPairDStream.mapWithState.

```
{"symbol":"CSGN","price":455.4870609871,"direction":"Sell","quantity":28,"exchange":"EUREX"}
{"symbol":"AAPL","price":311.5765300990,"direction":"Sell","quantity":14,"exchange":"FTSE"}
{"symbol":"UBSN","price":339.7060390450,"direction":"Buy","quantity":14,"exchange":"NASDAQ"}
{"symbol":"GOOG","price":264.5499525436,"direction":"Sell","quantity":59,"exchange":"FTSE"}
```
Various real-time analytics will be put in place for metrics like: avg price and quantity per symbol and direction. 

## How to run it
Inside Intellij, simply run DataStreamingTCPServersRunner and StreamingRunner.

Outside Intellij:

```unix
    cd TCPDataStreaming
    mvn package
    java -jar target/TCPDataStreaming-0.0.1-SNAPSHOT-jar-with-dependencies.jar
    ...
    cd SparkStreaming
    mvn package
    java -jar target/SparkStreaming-0.0.1-SNAPSHOT-jar-with-dependencies.jar
```

## Development environment and tools
- Ubuntu.
- Intellij.
- Spark 2.4.
- Kotlin 1.3.20.
