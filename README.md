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
        <module>CassandraMicroservice</module>
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
    docker run --name cassandra-microservice --network=host nicolanardino/cassandra-microservice:2.0
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
  cassandra-microservice:
    image: nicolanardino/cassandra-microservice:2.0
    container_name: cassandra-microservice
    depends_on:
      - cassandra-db
    restart: always
    network_mode: "host"
```

```unix
docker-compose up -d
```

### Cassandra
Objects set up at application start-up:

```sql
create KEYSPACE spark_data with replication={'class':'SimpleStrategy', 'replication_factor':1};
use spark_data;
create table if not exists spark_data.trade(symbol text, direction text, quantity int, price double, exchange text, timestamp timeuuid, primary key (exchange, direction, symbol, timestamp));

```
The above column family definition is to be able to query on exchange, direction and symbol, ordered by timestamp (UUID). 

#### Cassandra (Spring Boot) Microservice
Mirroring the spark_data.trade compound key (partition and cluster components), it allows the following:

```unix
curl localhost:9100/cassandra/getAllTrades
curl localhost:9100/cassandra/getTradesByExchange/FTSE
curl localhost:9100/cassandra/getTradesByExchangeAndDirection/FTSE/Sell
curl localhost:9100/cassandra/getTradesByExchangeAndDirectionAndSymbol/FTSE/Buy/UBS
```
Swagger support at http://localhost:9100/swagger-ui.html.

#### Cassandra Connections

Throughout the project, Cassadra connections are established in 3 ways:

- Direct connection:
 ```unix 
    Cluster.builder().addContactPoint(node).withPort(port).build()
 ```
- Spring Boot Data Cassandra: CassandraMicroservice.
- Spark Cassandra Connector: 
 ```unix 
    val sparkSession = SparkSession.builder().master("local[*]").appName("CassandraSparkConnector")
                    .config("spark.cassandra.connection.host", getProperty("cassandra.node"))
                    .config("spark.cassandra.connection.port", getProperty("cassandra.port")).getOrCreate()
 ```


## Development environment and tools
- Ubuntu.
- Intellij.
- Spark 2.4.1. 
- Kotlin 1.3.21. 
- Cassandra.
- Docker/ Docker Compose.
- Spring Boot. 
