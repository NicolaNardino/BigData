# Welcome to BigData.Spark

It's about various BigData problems solved with Spark and NoSQL technologies.

## Word count and ranking of multiple data streams
The first one of the most recurrent BigData problems: the word count and ranking.
Set up:
- A Spark Streaming application (SPA) connects to two TCP servers.
- Once the connection is established, the TCP servers start sending data, in the form of randomly generated phrases.
- The SPA aggregates the data by (full-outer) joining the two input streams and finally builds a ranking. 

The beauty of this is that the SPA can continuously receive and apply transformation to the received data, grouping it in pre-defined time frames (batch duration).
Unfortunately, Spark 2.4 doesn't fully support Java 10-11, for instance, in collect operations, so I'd to use Java 8 in the SPA. While I could use Java 11 in the TCP data streaming servers.

## Development environment and tools
Ubuntu.
Eclipse 12-2018.
Spark 2.4.
Java 8, 11.
