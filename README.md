# Welcome to BigData.Spark

It's about various BigData problems solved with Spark and NoSQL technologies. 
The server components are packaged in Docker images, published to my DockerHub [account](https://hub.docker.com/u/nicolanardino).

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

Furthermore, there's a multi-module Maven project: 
```unix
    <modules>
        <module>Utility</module>
        <module>TCPDataStreaming</module>
        <module>SparkStreaming</module>
    </modules>
```

Which builds both executable projects along with their Utility dependency:

```unix
    cd <application root>
    mvn package
    java -jar target/TCPDataStreaming-1.0-jar-with-dependencies.jar
    ...
    java -jar target/SparkStreaming-1.0-jar-with-dependencies.jar
```
### With Docker
Running mvn package in the parent pom, it creates Docker images for both the TCP Data Streaming Servers and the Spark Streaming Server, which can then be run by:

```unix
    docker run --name cassandra-db -v ~/data/docker/cassandra:/var/lib/cassandra --network=host cassandra:latest
    docker run --name tcp-data-streaming --network=host nicolanardino/tcp-data-streaming:2.0
    docker run --name spark-streaming --network=host nicolanardino/spark-streaming:2.0
```
Or with Docker Compose:

```unix
version: '3.4'

services:
  cassandra-db:
    image: cassandra:latest
    container_name: cassandra-db
    restart: always
    network_mode: "host"
    volumes:
      - ~/data/docker/cassandra:/var/lib/cassandra
  tcp-data-streaming:
    image: nicolanardino/tcp-data-streaming:2.0
    container_name: tcp-data-streaming
    depends_on:
      - cassandra-db
    restart: always
    network_mode: "host"
  spark-streaming:
    image: nicolanardino/spark-streaming:1.0
    container_name: spark-streaming
    depends_on:
      - tcp-data-streaming
    restart: always
    network_mode: "host"
```
```unix
docker-compose up -d
```

### Cassandra
At the moment,the interaction with Cassandra is at an early stage. In order to have it working, keyspace and table have to be created manually as follows

```sql
create KEYSPACE spark_data with replication={'class':'SimpleStrategy', 'replication_factor':1};
use spark_data;
create table Trade(symbol text, direction text, quantity int, price double, exchange text, timestamp timestamp, primary key (timestamp));

```

## Development environment and tools
- Ubuntu.
- Intellij.
- Spark 2.4.
- Kotlin 1.3.20.
- Cassandra.
- Docker/ Docker Compose.
