# BigData

A collection of big data problems solved with Apache Spark and NoSQL stores, written in Kotlin. It covers real-time stream processing with Spark Streaming, batch analytics over Cassandra, a Spring Boot microservice exposing Cassandra data, a Redis-backed distributed cache, and a first look at Delta Lake.

The server components are packaged as Docker images and published to [Docker Hub](https://hub.docker.com/u/nicolanardino).

## Architecture

```
 TCPDataStreaming                 SparkStreaming
 ┌──────────────────┐   TCP     ┌──────────────────────────┐
 │ server :1239     │──────────▶│ joins both streams,      │
 │ server :1225     │──────────▶│ stateful aggregations    │
 └────────┬─────────┘           └──────────────────────────┘
          │ creates keyspace/table
          ▼
 ┌──────────────────┐           ┌──────────────────────────┐
 │ Cassandra        │◀──────────│ CassandraMicroservice    │ REST :9100
 │ spark_data.trade │           └──────────────────────────┘
 │                  │◀──────────  SparkCassandra (batch analytics)
 └──────────────────┘

 DistributedCache ───▶ Redis (single node or 6-node cluster)
 DeltaLake        ───▶ local Delta tables
```

## Modules

| Module | Description |
|---|---|
| `Utility` | Shared model (trades, directions, exchanges) and helpers. |
| `TCPDataStreaming` | Two TCP servers that stream randomly generated data: phrases for the word count, or JSON trades. On start-up it creates the Cassandra keyspace and table; `CassandraRunner` seeds sample trades. |
| `SparkStreaming` | Spark Streaming application that connects to both TCP servers, joins the streams and applies stateful aggregations. |
| `SparkCassandra` | Batch Spark analytics over the Cassandra trade table via the Spark Cassandra Connector, for example average quantity per symbol, direction and exchange. |
| `CassandraMicroservice` | Spring Boot REST service over the Cassandra trade table, with Swagger UI. |
| `DistributedCache` | Redisson wrappers for a single Redis node and a Redis Cluster, used to cache and shard time series. |
| `DeltaLake` | Minimal example writing, appending to and reading a Delta Lake table with Spark. |

## Use cases

### Word count and ranking over multiple streams

The classic first big data problem.

1. The Spark Streaming application connects to two TCP servers.
2. Once connected, each server sends randomly generated phrases.
3. The application full-outer-joins the two streams, counts the words, and builds a ranking.

Spark Streaming processes the data continuously, grouping it into fixed time windows (the batch duration, 10 seconds by default). Stateful counts can use either `mapWithState` or `updateStateByKey`, selected by `StatefulAggregationType`.

### Trade analytics

The streaming servers send JSON trades (symbol, price, quantity, direction, exchange):

```json
{"symbol":"CSGN","price":455.4870609871,"direction":"Sell","quantity":28,"exchange":"EUREX"}
{"symbol":"AAPL","price":311.5765300990,"direction":"Sell","quantity":14,"exchange":"FTSE"}
{"symbol":"UBSN","price":339.7060390450,"direction":"Buy","quantity":14,"exchange":"NASDAQ"}
{"symbol":"GOOG","price":264.5499525436,"direction":"Sell","quantity":59,"exchange":"FTSE"}
```

The streaming application decodes them into `Trade` objects and applies stateful transformations with `JavaPairDStream.mapWithState`, keyed by symbol. Planned real-time metrics include average price and quantity per symbol and direction.

Trades stored in the Cassandra `trade` table are analysed in batch by `SparkCassandra` and exposed over REST by `CassandraMicroservice`.

## Tech stack

- Kotlin 1.3 on Java 8, Maven (multi-module)
- Apache Spark 2.4 (Spark Streaming, Spark SQL), Scala 2.12 binaries
- Apache Cassandra, DataStax Java driver 3.x, Spark Cassandra Connector
- Spring Boot 2.1 with Spring Data Cassandra, Swagger (Springfox)
- Redis and Redisson
- Delta Lake 0.3
- Docker and Docker Compose (images built with the fabric8 `docker-maven-plugin`)
- JUnit 5

## Getting started

### Prerequisites

- JDK 8 and Maven 3
- Docker, for the containerised setup or to run Cassandra and Redis locally

### Build

From the repository root:

```bash
mvn package
```

This builds every module and its `Utility` dependency, producing executable fat jars, and builds the Docker images.

### Run from the IDE

Start Cassandra (and Redis if needed), then run `DataStreamingTCPServersRunner` followed by `StreamingRunner`. The TCP servers stream trades by default; pass `PHRASE` as the first argument to both runners to switch to the word count.

### Run the jars

```bash
java -jar TCPDataStreaming/target/TCPDataStreaming-2.0-jar-with-dependencies.jar
java -jar SparkStreaming/target/SparkStreaming-2.0-jar-with-dependencies.jar
```

### Run with Docker

```bash
docker run --name redis-cache --network=host -v ~/data/docker/redis:/data -d redis redis-server --appendonly yes
docker run --name cassandra-db --network=host -v ~/data/docker/cassandra:/var/lib/cassandra -d cassandra:latest
docker run --name tcp-data-streaming --network=host nicolanardino/tcp-data-streaming:2.0
docker run --name spark-streaming --network=host nicolanardino/spark-streaming:2.0
docker run --name cassandra-microservice --network=host nicolanardino/cassandra-microservice:2.0
```

### Run with Docker Compose

[`docker-compose.yml`](docker-compose.yml) starts Redis, Cassandra, the TCP streaming servers, the Spark Streaming application, the Cassandra microservice and the distributed cache, all on the host network, with data volumes under `~/data/docker`:

```bash
docker-compose up -d
```

### Configuration

| File | Key settings |
|---|---|
| `TCPDataStreaming/src/main/resources/server.properties` | Server ports (`1239,1225`), uptime window, send delay, Cassandra connection and table definition |
| `SparkStreaming/src/main/resources/spark_streaming.properties` | Stream hosts and ports, batch duration, checkpoint directory, processing timeout |
| `SparkCassandra/src/main/resources/application.properties` | Cassandra node, port, keyspace and table |
| `CassandraMicroservice/src/main/resources/application.properties` | Service port (`9100`) and Cassandra connection |

## Cassandra

Objects created at application start-up:

```sql
CREATE KEYSPACE spark_data WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS spark_data.trade (
  symbol    text,
  direction text,
  quantity  int,
  price     double,
  exchange  text,
  timestamp timeuuid,
  PRIMARY KEY (exchange, direction, symbol, timestamp)
);
```

`exchange` is the partition key and `direction`, `symbol` and `timestamp` are clustering columns. Trades can therefore be queried by exchange, then narrowed by direction and symbol, and come back ordered by time.

### Cassandra microservice

The REST endpoints mirror the compound key, so each query is an efficient partition or clustering-prefix lookup:

```bash
curl localhost:9100/cassandra/getAllTrades
curl localhost:9100/cassandra/getTradesByExchange/FTSE
curl localhost:9100/cassandra/getTradesByExchangeAndDirection/FTSE/Sell
curl localhost:9100/cassandra/getTradesByExchangeAndDirectionAndSymbol/FTSE/Buy/UBSN
```

Swagger UI: http://localhost:9100/swagger-ui.html

### Connecting to Cassandra

The project connects to Cassandra in three ways:

- **DataStax driver, directly** (`TCPDataStreaming`):

  ```kotlin
  Cluster.builder().addContactPoint(node).withPort(port).build()
  ```

- **Spring Data Cassandra** (`CassandraMicroservice`).
- **Spark Cassandra Connector** (`SparkCassandra`):

  ```kotlin
  val sparkSession = SparkSession.builder().master("local[*]").appName("CassandraSparkConnector")
      .config("spark.cassandra.connection.host", getProperty("cassandra.node"))
      .config("spark.cassandra.connection.port", getProperty("cassandra.port"))
      .getOrCreate()
  ```

## Redis Cluster

`DistributedCache` is tested against a local Redis Cluster of three masters and three replicas. To set one up:

1. Build Redis from source into `$REDIS_HOME` and add `$REDIS_HOME/src` to `PATH`.
2. Create a cluster folder, `$REDIS_CLUSTER_HOME`, containing six node folders named after their ports, `7000` to `7005`, as the Redis cluster tutorial suggests.
3. In each node folder, create a minimal `redis.conf`, changing `port` to match the folder:

   ```
   port 7000
   cluster-enabled yes
   cluster-config-file nodes.conf
   cluster-node-timeout 5000
   appendonly yes
   ```

4. Start the six instances:

   ```bash
   #!/bin/bash
   cd "$REDIS_CLUSTER_HOME"
   for port in 7000 7001 7002 7003 7004 7005; do
     (cd "$port" && redis-server ./redis.conf &)
     sleep 3
   done
   ```

5. Create the cluster (a one-off step), with one replica per master:

   ```bash
   redis-cli --cluster create 127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 \
     127.0.0.1:7003 127.0.0.1:7004 127.0.0.1:7005 --cluster-replicas 1
   ```

## Development environment

- Ubuntu, IntelliJ IDEA

## Status

This is a learning project, last developed around 2019 and no longer maintained beyond dependency bumps. Spark 2.4, Kotlin 1.3 and Spring Boot 2.1 are all end-of-life, and the DStream-based Spark Streaming API has been superseded by Structured Streaming. Reviving it would start with an upgrade to Spark 3.x and Structured Streaming.

## License

See [LICENSE](LICENSE).
