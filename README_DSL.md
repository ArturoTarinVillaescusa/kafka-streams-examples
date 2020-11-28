# STREAMS DSL

# Stateless transformations

## Creating source streams from Kafka

### input topic -> KStream:

* [stateless/O1_KStreamFromTopic.java](src/main/java/io/confluent/examples/streams/streamdsl/stateless/O1_KStreamFromTopic.java)
* [stateless/O1_KStreamFromTopicTest.java](src/test/java/io/confluent/examples/streams/streamdsl/stateless/O1_KStreamFromTopicTest.java)

### input topic -> KTable:

* [stateless/O2_KTableFromTopic.java](src/main/java/io/confluent/examples/streams/streamdsl/stateless/O2_KTableFromTopic.java) 
* [stateless/O2_KTableFromTopicTest.java](src/test/java/io/confluent/examples/streams/streamdsl/stateless/O2_KTableFromTopicTest.java) 

### input topic -> GlobalKTable:

* [stateless/O3_GlobalKTableFromTopic.java](src/main/java/io/confluent/examples/streams/streamdsl/stateless/O3_GlobalKTableFromTopic.java) 
* [stateless/O3_GlobalKTableFromTopicTest.java](src/test/java/io/confluent/examples/streams/streamdsl/stateless/O3_GlobalKTableFromTopicTest.java) 

### branch: KStream -> KStream[]

Branch (or split) a KStream based on the supplied predicates into one or more KStream instances.

Predicates are evaluated in order. A record is placed to one and only one output stream on the first match:
if the n-th predicate evaluates to true, the record is placed to n-th stream. If no predicate matches,
the the record is dropped.

Branching is useful, for example, to route records to different downstream topics.

* [stateless/O4_branch.java](src/main/java/io/confluent/examples/streams/streamdsl/stateless/O4_branch.java) 
* [stateless/O4_branchTest.java](src/test/java/io/confluent/examples/streams/streamdsl/stateless/O4_branchTest.java) 

### filter: KStream -> KStream /  KTable -> KTable

Evaluates a boolean function for each element and retains those for which the function returns true.


src/main/java/io/confluent/examples/streams/streamdsl/stateless/O5_filter.java 
src/test/java/io/confluent/examples/streams/streamdsl/stateless/O5_filterTest.java 


### filter: KStream -> KStream /  KTable -> KTable
                                                                                                                                                                    
Evaluates a boolean function for each element and drops those for which the function returns true. 


17:50:04 $ ll src/test/java/io/confluent/examples/streams/streamdsl/O6_filterNotTest.java 
-rw-rw-r-- 1 4394 nov  3 17:17 src/test/java/io/confluent/examples/streams/streamdsl/O6_filterNotTest.java

17:50:52 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateless/O6_filterNot.java 
-rw-rw-r-- 1 6051 nov  3 17:26 src/main/java/io/confluent/examples/streams/streamdsl/stateless/O6_filterNot.java


### flatMap: KStream -> KStream

Takes one record and produces zero, one, or more records. You can modify the record keys and values, including their types.

Marks the stream for data re-partitioning: Applying a grouping or a join after flatMap will result in re-partitioning of the records.
If possible use flatMapValues instead, which will not cause data re-partitioning.

17:10:08 $ ll src/test/java/io/confluent/examples/streams/streamdsl/O7_flatmapTest.java 
-rw-rw-r-- 1 3875 nov  3 17:49 src/test/java/io/confluent/examples/streams/streamdsl/O7_flatmapTest.java

17:49:57 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateless/O7_flatMap.java 
-rw-rw-r-- 1 6519 nov  3 17:41 src/main/java/io/confluent/examples/streams/streamdsl/stateless/O7_flatMap.java

### flatMapValues: KStream -> KStream

Takes one record and produces zero, one, or more records, while retaining the key of the original record. You can modify the record values and the value type.

flatMapValues is preferable to flatMap because it will not cause data re-partitioning. However, you cannot modify the key or key type like flatMap does.


17:51:00 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateless/O8_flatMapValues.java 
-rw-rw-r-- 1 6301 nov  4 06:43 src/main/java/io/confluent/examples/streams/streamdsl/stateless/O8_flatMapValues.java

06:49:22 $ ll src/test/java/io/confluent/examples/streams/streamdsl/O8_flatMapValuesTest.java 
-rw-rw-r-- 1 3754 nov  4 06:47 src/test/java/io/confluent/examples/streams/streamdsl/O8_flatMapValuesTest.java

**Note:**
> flatMap allows you to modify the keys and key type
>
> flatMapValues doesn't allow modifying neither the keys or key type

### foreach: KStream -> void / KTable -> void

Terminal operation. Performs a stateless action on each record.

You would use foreach to cause side effects based on the input data (similar to peek) and then stop further processing of the
input data (unlike peek, which is not a terminal operation).

Note on processing guarantees: Any side effects of an action (such as writing to external systems) are not trackable by Kafka,
which means they will typically not benefit from Kafka’s processing guarantees.


06:49:36 $ ll src/test/java/io/confluent/examples/streams/streamdsl/OO9_foreachTest.java 
-rw-rw-r-- 1 3989 nov  4 06:59 src/test/java/io/confluent/examples/streams/streamdsl/OO9_foreachTest.java

07:02:02 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateless/OO9_foreach.java 
-rw-rw-r-- 1 6242 nov  4 07:00 src/main/java/io/confluent/examples/streams/streamdsl/stateless/OO9_foreach.java

### groupByKey: KStream -> KGroupedStream

It groups the records by the existing key. Grouping is a pre-requisite for aggregate a stream or a table. Grouping ensures
that data is properly partitioned ("keyed") for subsequent operations.

When to set explicit SerDes? Variants of groupByKey exist to override the default SerDes of your application, which you MUST DO
if the key and/or the value types of the resulting KGroupedStream do not match the default SerDes.

**NOTE:** 
>grouping vs windowing
>
>windowing -> lets you control of how "sub-group" the grouped records of the same key into so called "windows" for stateful
operations such as windowed aggregations or windowed joins.
>
>groupByKey causes data re-partitioning if and only if the stream was previously marked for re-partitioning. groupByKey is
preferable to groupBy because it re-partitions data only if it was already marked to do so. However groupByKey doesn't allow you
to modify the keys or the key type, so if you need to do it, use groupBy instead, which allows this action.


07:02:22 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateless/O10_groupByKey.java 
-rw-rw-r-- 1 7406 nov  4 10:24 src/main/java/io/confluent/examples/streams/streamdsl/stateless/O10_groupByKey.java

07:01:15 $ ll src/test/java/io/confluent/examples/streams/streamdsl/O10_groupByKeyTest.java 
-rw-rw-r-- 1 4185 nov  4 10:19 src/test/java/io/confluent/examples/streams/streamdsl/O10_groupByKeyTest.java

### groupBy: KStream -> KGroupedStream / KTable -> KGroupedTable

Groups the records by a new key, which may of a different key type. When grouping a table you may also specify a new value and value type.

groupBy is a shorthand of selectKey(...).groupByKey()

Grouping is a pre-requisite for aggregating a stream or a table and ensures that data is properly partitioned ("keyed") for subsequent
operations.

When to set explicit SerDes? Variants of groupBy exists to override the configured default SerDes of your application, WHICH YOU MUST DO
if the key and/or value types of the resulting KGroupedStream or KGroupedTable do not match the configured default SerDes.

**NOTE:**
>grouping vs windowing -> A related operation is windowing, which lets you control how to "sub-group" the grouped records of the same
key into so-called windows for stateful operations such as windowed aggregations or windowed joins.
>
>groupBy always causes data re-partitioning. If you don't need to modify the key or key type use groupByKey, which re-partition data only
if required.


10:25:13 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateless/O11_groupBy.java 
-rw-rw-r-- 1 6244 nov  4 11:30 src/main/java/io/confluent/examples/streams/streamdsl/stateless/O11_groupBy.java


10:25:01 $ ll src/test/java/io/confluent/examples/streams/streamdsl/O11_groupByTest.java 
-rw-rw-r-- 1 4409 nov  4 11:33 src/test/java/io/confluent/examples/streams/streamdsl/O11_groupByTest.java

**Note:**
>groupBy allows you to modify the keys and key type

>groupByKey doesn't allow modifying neither the keys or key type

### cogroup: KGroupedStream -> CogroupedKStream / CogroupedKStream → CogroupedKStream

Cogrouping enables aggregating multiple input streams in a single operation. The different and already aggregated
input streams must have the same key type and may have different value types.

KStream#cogroup() creates a new cogrouped stream with a single input stream
CogroupedKStream#cogroup() adds a grouped stream to an existing cogrouped stream

Because each KGroupedStream may have different value type, individual "adder" aggregator must be provided
via cogroup(); those aggregators will be used by the downstream aggregate() operator.

A CogroupedKStream may be windowed before it is aggregated.

Cogroup doesn't cause repartition as it has the pre-requisite that the input streams are grouped.

In the process of creating these groups they will have already been repartitioned if the stream was already
marked for repartitioning.


12:37:47 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateless/O12_cogroup.java 
-rw-rw-r-- 1 7018 nov  4 16:52 src/main/java/io/confluent/examples/streams/streamdsl/stateless/O12_cogroup.java

12:39:23 $ ll src/test/java/io/confluent/examples/streams/streamdsl/O12_cogroupTest.java 
-rw-rw-r-- 1 4962 nov  4 16:29 src/test/java/io/confluent/examples/streams/streamdsl/O12_cogroupTest.java

### Map: KStream -> KStream

Takes one record and produces one record. You can modify keys, values and their types too.

Marks the stream for data re-partitioning. Applying a grouping or a join after the map will
result in re-partitioning on the records. If possible use mapValues instead, which doesn't cause
data re-partitioning.

16:53:26 $ ll src/test/java/io/confluent/examples/streams/streamdsl/O13_mapTest.java 
-rw-rw-r-- 1 3888 nov  5 06:43 src/test/java/io/confluent/examples/streams/streamdsl/O13_mapTest.java


16:53:33 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateless/O13_map.java 
-rw-rw-r-- 1 6530 nov  5 06:42 src/main/java/io/confluent/examples/streams/streamdsl/stateless/O13_map.java

### mapValues: KStream -> KStream / KTable -> KTable

Takes one record and produces one record while retaining the key of the original record.

You can modify the record value and value type. mapValues is preferable to map because it will not cause data-repartitioning.
However it does not allow you to modify the key or the key type like map does.

Note that it is possible though to get read-only access to the input record key if you use ValueMapperWithKey instead
of ValueMapper


06:43:34 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateless/O14_mapValues.java 
-rw-rw-r-- 1 6416 nov  5 07:07 src/main/java/io/confluent/examples/streams/streamdsl/stateless/O14_mapValues.java

06:43:27 $ ll src/test/java/io/confluent/examples/streams/streamdsl/O14_mapValuesTest.java 
-rw-rw-r-- 1 3934 nov  5 07:11 src/test/java/io/confluent/examples/streams/streamdsl/O14_mapValuesTest.java

**Note:**
>map allows you to modify the keys and key type
>
>mapValues doesn't allow modifying neither the keys or key type

### merge: KStream -> KStream

Merges records of two streams into one larger stream. 

There is NO ORDER GUARANTEE between records from different streams in the merged stream. Relative order is preserved
within each input stream though, i.e. records within the same input are processed in order. 

07:11:24 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateless/O15_merge.java 
-rw-rw-r-- 1 5952 nov  5 08:07 src/main/java/io/confluent/examples/streams/streamdsl/stateless/O15_merge.java

07:11:19 $ ll src/test/java/io/confluent/examples/streams/streamdsl/O15_mergeTest.java 
-rw-rw-r-- 1 4164 nov  5 08:10 src/test/java/io/confluent/examples/streams/streamdsl/O15_mergeTest.java

### peek: KStream -> KStream

Performs a stateless action on each record and returns unchanged stream.

You would use peek to cause side effects based on the input data, similar to what foreach does, and continue processing
the input data, unlike foreach does (which is a terminal operation).

Peek returns the input stream as-is: if you need to modify the stream use map or mapValues instead.

Peek is helpful for those us cases such as logging or tracking metrics or for debugging and troubleshooting.

Note on processing guarantees: any side effects of an action, such as writing on external systems, are not trackable
with Kafka, which means they will typically not benefit from Kafka's processing guarantees.

08:10:15 $ ll src/test/java/io/confluent/examples/streams/streamdsl/O16_peekTest.java 
-rw-rw-r-- 1 4426 nov  5 11:22 src/test/java/io/confluent/examples/streams/streamdsl/O16_peekTest.java

08:10:26 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateless/O16_peek.java 
-rw-rw-r-- 1 6051 nov  5 11:22 src/main/java/io/confluent/examples/streams/streamdsl/stateless/O16_peek.java

### print: KStream -> void

Terminal operation. Prints the records to System.out or into a file.

Calling print(Printed.toSysOut()) is the same as calling foreach((key, value) -> System.out.println(key + ", " + value))

13:02:16 $ ll src/test/java/io/confluent/examples/streams/streamdsl/stateless/O10_groupByKeyTest.java 
-rw-rw-r-- 1 4475 nov  5 12:56 src/test/java/io/confluent/examples/streams/streamdsl/stateless/O10_groupByKeyTest.java

11:23:15 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateless/stateless/O10_groupByKey.java 
-rw-rw-r-- 1 7645 nov  5 13:01 src/main/java/io/confluent/examples/streams/streamdsl/stateless/O10_groupByKey.java

11:23:08 $ cat /tmp/print-stream-example.txt 
[KTABLE-TOSTREAM-0000000010]: 1, 1
[KTABLE-TOSTREAM-0000000010]: 1, 2
[KTABLE-TOSTREAM-0000000010]: 2, 1
[KTABLE-TOSTREAM-0000000010]: 2, 2
[KTABLE-TOSTREAM-0000000010]: 2, 3

13:03:29 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateless/O17_print.java 
-rw-rw-r-- 1 6160 nov  5 13:11 src/main/java/io/confluent/examples/streams/streamdsl/stateless/O17_print.java

13:11:49 $ ll src/test/java/io/confluent/examples/streams/streamdsl/stateless/O17_printTest.java 
-rw-rw-r-- 1 4444 nov  5 12:56 src/test/java/io/confluent/examples/streams/streamdsl/stateless/O17_printTest.java

13:03:01 $ cat /tmp/print-stream-example.txt 
[KSTREAM-MERGE-0000000002]: [B@7f77e91b, World!
[KSTREAM-MERGE-0000000002]: [B@8e0379d, you?
[KSTREAM-MERGE-0000000002]: [B@341b80b2, thank you.

### repartition: KStream -> KStream

Manually triggers re-partitioning of the stream with the specified number of partitions.

The repartition() method is similar to through(), but in the case of repartition is Kafka who manages the topic for you.

The generated topic is treated as an internal topic, so data is purged automatically, as with any other internal re-partitioning
topic.

You can specify the number of partitions, which enables scaling downstream sub-topologies in and out.

The reartition() operation always triggers re-partitioning of the stream, so you can use it with embedded processor API methods,
like transform(), that don't trigger auto re-partitioning when a key-changing operation is performed beforehand.



13:12:26 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateless/O18_repartition.java 
-rw-rw-r-- 1 5721 nov  5 13:38 src/main/java/io/confluent/examples/streams/streamdsl/stateless/O18_repartition.java

13:12:19 $ ll src/test/java/io/confluent/examples/streams/streamdsl/stateless/O18_repartitionTest.java 
-rw-rw-r-- 1 3589 nov  5 13:39 src/test/java/io/confluent/examples/streams/streamdsl/stateless/O18_repartitionTest.java

### selectKey: KStream -> KStream

Assigns a new key and possibly a new key type also to each record of a stream.

Calling selectKey(mapper) is the same as calling map((key, value) -> mapper(key, value), value)

Marks the stream for data re-partitioning: applying a grouping or a join after selectKey will result in re-partitioning
of the records.

13:39:49 $ ll src/test/java/io/confluent/examples/streams/streamdsl/stateless/O19_selectKeyTest.java 
-rw-rw-r-- 1 3825 nov  5 13:58 src/test/java/io/confluent/examples/streams/streamdsl/stateless/O19_selectKeyTest.java

13:39:56 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateless/O19_selectKey.java 
-rw-rw-r-- 1 6222 nov  5 14:04 src/main/java/io/confluent/examples/streams/streamdsl/stateless/O19_selectKey.java

## Stream to Table: KStream -> KTable

Convert an event stream into a table or a changelog stream.

KStream<byte[], String> stream = ...;

KTable<byte[], String> table = stream.toTable();

## Table to Stream: KTable -> KStream

Get the changelog stream of this table.

KTable<byte[], String> table = ...;

// Also, a variant of `toStream` exists that allows you
// to select a new key for the resulting stream.
KStream<byte[], String> stream = table.toStream();

# Stateful transformations

Depend on state for processing inputs and producing outputs and require a STATE STORE associated with the stream processor.

For example, in aggregating operations, a windowing state store is used to collect the latest aggregation results per window.

In join operations, a windowing state store is used to collect all of the records received so far within the defined window
boundary.

Note that state stores are fault-tolerant. In case of failure, Kafka Streams guarantees to fully restore all state stores
prior to resuming the processing.

Available stateful transformations in the DSL include:

* Aggregating
* Joining
* Windowing as part of aggregations and joins
* Applying custom processors and transformers, with may be stateful for Processor API integration.

This diagram shows the relationships between the stateful transformations:

https://docs.confluent.io/current/_images/streams-stateful_operations.png

## WordCount: A stateful application example


14:04:23 $ ll src/main/java/io/confluent/examples/streams/WordCountLambdaExample.java 
-rw-rw-r-- 1 11326 nov  5 14:31 src/main/java/io/confluent/examples/streams/WordCountLambdaExample.java

14:04:31 $ ll src/test/java/io/confluent/examples/streams/WordCountLambdaExampleTest.java 
-rw-rw-r-- 1 4996 nov  5 14:26 src/test/java/io/confluent/examples/streams/WordCountLambdaExampleTest.java

## Stateful Aggregating operations

After records are grouped by key via groupByKey or groupBy, and thus represented as either a KGroupedStream or
KGroupedTable, they can be aggregated via an operation such as reduce()

Aggregations are key-based operations, which means that they always operate over records of the same key.

You can perform aggregations on windowed or non-windowed data.

**IMPORTANT NOTE**

>To support fault tolerance and avoid undesirable behavior, the initializer and aggregator must be stateless.
>
>The aggregation results should be passed in the return value of the initializer and aggregator.

>Do not use class member variables because data can potentially get lost in case of failure.


### aggregate: KGroupedStream -> KTable / CogroupedKStream -> KTable / KGroupedTable -> KTable

**R|o|l|l|i|n|g| |a|g|g|r|e|g|a|t|i|o|n**

aggregates the values of non-windowed records by the grouped key.

Aggregating is a generalization of reduce, and allows for example the aggregate value to have a different type
than the input values.

When aggregating a grouped stream you must provide an initializer, e.g. aggValue = 0, and an "adder" aggregator,
e.g. aggValue + currValue.

When aggregating a cogrouped stream you must only provide an initializer, because the corresponding aggregators
are provided in the prior cogroup() calls already.

When aggregating a grouped table you must provide an initializer, an adder and a subtractor, e.g. aggValue - oldValue

Detailed behavior of KGroupedStream and CogroupedKStream:

* Input records with null keys are ignored
* When a record key is received for the first time, the initializer is called
* When the stream starts, the initializer is called before the adder is called
* Whenever a record with non-null value is received, the adder is called

Detailed behavior of KGroupedTable:

* Input records with null keys are ignored
* When a record key is received for the first time, the initializer is called
* When the stream starts, the initializer is called before the adder and the subtractor are called
* When the first non-null value is received for a key, e.g. INSERT, then only the adder is called
* When subsequent non-null values are received for a key, e.g. UPDATE, then:
  1. The subtractor is called with the old value as stored in the table
  2. The adder is called with the new value of the input record that was just received. The order of
     execution for the subtractor and adder is not defined
* When a tombstone, i.e. a record with a null value, is received for a key, e.g. DELETE, then only the
  subtractor is called. Note that whenever the subtractor returns a null value itself, then the corresponding
  key is removed from the resulting KTable. If that happens, any next input record for that key will trigger
  the initializer again.
  So, in contrast to KGroupedStream, over time the KGroupedTable initializer may be called more than once
  for a key


14:31:25 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O01_aggregate.java 
-rw-rw-r-- 1 6990 nov  6 06:29 src/main/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O01_aggregate.java

14:31:49 $ ll src/test/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O01_aggregateTest.java 
-rw-rw-r-- 1 3959 nov  6 06:37 src/test/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O01_aggregateTest.java



06:40:56 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O03_aggregateTable.java 
-rw-rw-r-- 1 7563 nov  6 13:48 src/main/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O03_aggregateTable.java

06:40:56 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O03_aggregateTable.java 
-rw-rw-r-- 1 7563 nov  6 13:48 src/main/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O03_aggregateTable.java



14:08:18 $ ll src/test/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O02_aggregateStreamTest.java 
-rw-rw-r-- 1 4361 nov  6 12:32 src/test/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O02_aggregateStreamTest.java


14:08:23 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O02_aggregateStream.java 
-rw-rw-r-- 1 7535 nov  6 14:07 src/main/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O02_aggregateStream.java

### windowedBy (aggregate windowed): 
    KGroupedStream → TimeWindowedStream; TimeWindowedStream → KTable
    KGroupedStream → SessionWindowedStream; SessionWindowedStream → KTable
    CogroupedKStream → TimeWindowedCogroupedStream; TimeWindowedCogroupedStream → KTable
    CogroupedKStream → SessionWindowedCogroupedStream; SessionWindowedCogroupedStream → KTable


**W|i|n|d|o|w|e|d| |a|g|g|r|e|g|a|t|i|o|n**
 
aggregates the values of records, per window, by the grouped key.

Aggregating is a generalization of reduce and allows, for example, the aggregate value to have a different type than the
input values. 

When aggregating a grouped stream by window you must provide:
* an initializer (e.g. aggValue = 0)
* an adder aggregator (e.g. aggValue + curValue)
* and a window

When aggregating a cogrouped stream by window you must provide:
* an initializer (e.g. aggValue = 0)
* and a window
because the corresponding adder aggregations are provided in the prior cogroup() calls already.

When windowing is based on sessions you must provide additionally:
* a session merger aggregator (e.g. mergedValue = leftValue + rightValue)

The windowBy windowed aggregate turns a TimeWindowedKStream<K, V> or SessionWindowedKStream<K, V> into a KTable<Windowed<K>, V>

Detailed behavior:

* The windowed aggregate behaves similar to the rolling aggregate. The additional twist is that the behavior applies per window.
* Input records with null keys are ignored in general.
* When a record is received for the first time for a given window, the initializer is called before the adder 
* Whenever a record with non-null value is received for a given window, the adder is called
* When using session windows the session merger is called whenever two sessions are being merged. 

05:53:33 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O04_windowedBy.java 
-rw-rw-r-- 1 13211 nov  7 14:46 src/main/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O04_windowedBy.java

14:10:41 $ ll src/test/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O04_windowedByTest.java 
-rw-rw-r-- 1 6847 nov  7 14:47 src/test/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O04_windowedByTest.java

### count: KGroupedStream → KTable / KGroupedTable → KTable

Rolling aggregation. Counts the number of records by grouped key.

Detailed behavior for KGroupedStream:

* Input records with null keys are ignored.

Detailed behavior for KGroupedTable:

* Input records with null keys are ignored.
* Input records with null values are not ingored but interpreted as tombstones for the corresponding key, indicating the deletion
  of the key from the table


14:48:30 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O05_countStream.java 
-rw-rw-r-- 1 5821 nov  7 21:33 src/main/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O05_countStream.java

14:48:18 $ ll src/test/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O05_countStreamTest.java 
-rw-rw-r-- 1 4409 nov  7 21:33 src/test/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O05_countStreamTest.java


21:33:45 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O06_countTable.java 
-rw-rw-r-- 1 5965 nov  7 21:39 src/main/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O06_countTable.java

21:33:38 $ ll src/test/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O06_countTableTest.java 
-rw-rw-r-- 1 4379 nov  7 21:31 src/test/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O06_countTableTest.java


count KTable throws an error:

org.apache.kafka.streams.errors.StreamsException: ClassCastException while producing data to topic
kstreams-from-topic-KTABLE-AGGREGATE-STATE-STORE-0000000004-repartition.
A serializer (key: org.apache.kafka.common.serialization.StringSerializer / value:
org.apache.kafka.streams.kstream.internals.ChangedSerializer) is not compatible to the actual key or value type 

### count windowed: 

    KGroupedStream → TimeWindowedStream; TimeWindowedStream → KTable
    KGroupedStream → SessionWindowedStream; SessionWindowedStream → KTable

Windowed aggregation. Counts the number of records per window by the grouped key.

The windowed count turns a TimeWindowedKStream<K, V> or a SessionWindowedKStream into a KTable<Windowed<K>, V>


21:40:09 $ ll src/test/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O07_windowedByCountTest.java 
-rw-rw-r-- 1 5491 nov  8 07:20 src/test/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O07_windowedByCountTest.java

21:40:17 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O07_windowedByCount.java 
-rw-rw-r-- 1 7022 nov  8 07:19 src/main/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O07_windowedByCount.java

### reduce: KGroupedStream → KTable / KGroupedTable → KTable

Rolling aggregation. Combines the values of non-windowed records by the grouped key.

The current record value is combined with the last reduced value, and the new reduced value is returned.

The result value type cannot be changed, unlike aggregate()

When reducing a grouped stream you must provide an "adder" reducer, e.g. aggValue + curValue.

When reducing a grouped table you must additionally provide a "subtractor" reducer, e.g. aggValue - oldValue.

Detailed behavior of KGroupedStream:

* Input records with null keys are ignored
* When a record key is received for the first time, then the value of that record is used as the initial aggregate value.
* Whenever a record with non-null value is received, the adder is called.

Detailed behavior of KGroupedTable:

* Input records with null keys are ignored
* When a record key is received for the first time, then the value of that record is used as the initial aggregate value.
  Note that in contrast to KGroupedStream, over time this initialization step may happen more than once for a key as a
  result of having received input tombstone records for that key.
* When the first non-null value is received for a key, e.g. INSERT, then only adder is called
* When subsequent non-null values are received for a key, e.g. UPDATE, then:
  1. The subtractor is called with the old value as stored in the table
  2. The adder is called with the new value of the input record that was just received.
  The order of execution of the adder and the subtractor is not defined
* When a tombstone record, i.e. a record with null value is received for a key, e.g. DELETE, then only the subtractor
  is called.
  Note that whenever the subtractor returns a null value itself, then the corresponding key is removed from the KTable.
  If that happens, any next input record for that key will re-initialize its aggregate value.



07:20:32 $ ll src/test/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O08_reduceStreamTest.java 
-rw-rw-r-- 1 4291 nov  8 08:21 src/test/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O08_reduceStreamTest.java


07:20:43 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O08_reduceStream.java 
-rw-rw-r-- 1 7028 nov  8 08:20 src/main/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O08_reduceStream.java


08:21:56 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O09_reduceTable.java 
-rw-rw-r-- 1 7272 nov  8 08:16 src/main/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O09_reduceTable.java


08:22:13 $ ll src/test/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O09_reduceTableTest.java 
-rw-rw-r-- 1 4025 nov  8 08:20 src/test/java/io/confluent/examples/streams/streamdsl/stateful/aggregating/O09_reduceTableTest.java

### reduce (windowed):

    KGroupedStream → TimeWindowedStream; TimeWindowedStream → KTable
    KGroupedStream → SessionWindowedStream; SessionWindowedStream → KTable

Windowed aggregation. Combines the values of records per window, by the grouped key.

The current value of the record is combined with the last reduced value, and new reduced value is returned.

Records with null key or value are ignored.

The result value type cannot be changed, unlike aggregate.

The windowed reduce turns a TimeWindowedKStream<K, V> or a SessionWindowedKStream<K, V> into a windowed KTable<K, V>

Detailed behavior:

* The windowed reduce behaves similar to the rolling reduce described above.
* The additional twist is that the behavior applies per window.
* Input records with null keys are ignored.
* When a record is received for the first time for a given window, then the value of that record is used as the initial aggregate value.
* Whenever a record with non-null value is received for a given window, the adder is called.


## Stateful Joining operations

Streams and tables can also be joined. Many streaming applications in practice are coded as streaming joins.

For example, applications backing an online shop might need to update multiple database tables, e.g. sales prices,
inventory and customer information, in order to enrich a new data record, e.g. customer transaction, with context
information.

That is, scenarios where you need to perform table lookups at very large scale and with a low processing latency.

Here, a popular pattern is to make the information in the databases available in Kafka through so-called Change Data
Capure (CDC) in combination with Kafka's Connect API, and then implementing applications that leverage the Streams API
to perform very fast and efficient local joins of such tables and streams, see this link to learn more about it, 
https://www.confluent.io/blog/distributed-real-time-joins-and-aggregations-on-user-activity-events-using-kafka-streams/, 
rather than requiring the application to make a query to a remote database over the network for each record.

In our example, the KTable concept in Kafka Streams would enable us to track the latest state, e.g. snapshot, of each
table in a local State Store, thus greatly reducing the processing latency as well as reducing the load of the remote
databases when doing such streaming joins.

The following join operations are supported:

Join operands 	        | Type 	       |  (INNER) JOIN  | LEFT JOIN 	| OUTER JOIN 	| Demo application
------------------------|--------------|----------------|---------------|---------------|-----------------------------------
KStream-to-KStream 	    | Windowed 	   |  Supported 	| Supported 	| Supported 	| StreamToStreamJoinIntegrationTest
KTable-to-KTable 	    | Non-windowed |  Supported 	| Supported 	| Supported 	| TableToTableJoinIntegrationTest
KStream-to-KTable 	    | Non-windowed |  Supported 	| Supported 	| Not Supported | StreamToTableJoinIntegrationTest
KStream-to-GlobalKTable | Non-windowed |  Supported 	| Supported 	| Not Supported | GlobalKTablesExample
KTable-to-GlobalKTable  | N/A 	       |  Not Supported | Not Supported | Not Supported | N/A

## Running the GlobalKTablesExample.java:

07:42:02 $ confluent local services schema-registry start

### Build the application:

07:42:02 $ mvn -DskipTests=true -Dcheckstyle.skip clean package

### Run the Kafka Streams topology:

07:42:02 $ java -cp target/kafka-streams-examples-6.0.0-standalone.jar \
                    io.confluent.examples.streams.GlobalKTablesExample

### Generate messages to the input topics:

07:42:02 $ java -cp target/kafka-streams-examples-6.0.0-standalone.jar \
                io.confluent.examples.streams.GlobalKTablesExampleDriver


## Running the GlobalKTablesExampleTest.java:

### Make sure you stop the external Kafka Confluent

07:55:13 $ confluent local services stop

### Make sure the 8080 port is not in use, otherwise the test will fail complaining about it:

20:15:48 $ ~/Downloads/apache-tomcat-9.0.39/bin/catalina.sh stop

### Run IntelliJ in debug mode:

07:55:13 $ run GlobalKTablesExampleTest.java in debug mode


## Join co-partitioning requirements:

Input data must be co-partitioned when joining. This ensures that input records with the same key, from both sides
of the join, are delivered to the same stream task during processing.

It is responsibility of the user to ensure data co-partitioning when joining.

Note: If possible consider GlobalKTables for joining because they don't require data co-partitioning.

The requirements of data co-partitioning are:

* The input topics of the join (left side and right side) must have the same number of partitions
* All applications that write to the input topics must have the same partitioning strategy so that the records with
  the same key are delivered to the same partition number.
  In other words: the keyspace of the input data must be distributed across partitions in the same manner.
  This means that, for example, applications that use Kafka's Producer API must use the same partitioner (cf. the
  producer setting "partition.class" aka "ProducerConfig.PARTITIONER_CLASS_CONFIG", and applications that use the
  Kafka's Streams API must use the same StreamPartitioner for operations such KStream#to().
  YOU DON'T NEED TO WORRY ABOUT THIS IF YOU USE THE DEFAULT PARTITIONER SETTINGS.

Co-partitioning is required for joins because KS-KS, KT-KT and KS-KT joins are performed based on the keys of records,
e.g. leftRecord.key == rightRecord.key

KS-GlobalKTable joins do not require co-partitioning because all the partitions of the GlobalKTable's underlying
changelog are made available to each KafkaStreams instance, i.e. each instance has a full copy of the changelog stream.
Further, a KeyValueMapper allows for non-key based joins from the KStream to the GlobalKTable

Kafka Streams partly verifies the co-partitioning requirement: During the partition assignment step, i.e. at runtime,
Kafka Streams verifies whether the number of partitions for both sides of a join are the same.

If they aren't, a TopologyBuilderException is being thrown.

Note that Kafka Streams cannot verify whether the partitioning strategy matches between the input streams/tables of a
join. It's up to the user to ensure that this is the case.

## Ensuring data co-partitioning:

If the inputs of a join are not co-partitioned yet, you must ensure this manually.
You may follow a procedure such as outlined below.  

In order to avoid bottlenecks it is recommended repartition the topic with fewer partitions to match the larger partition
number.

It's also possible to repartition the topic with more partitions to match the smaller partition number.

For KS-KT joins it is recommended repartitioning the KStream, because repartitioning the KTable may result
in a second State Store.

For KT-KT joins consider the size of the KTables (number or records?) and repartition the smaller KTable.

1. Identify the input KStream/KTable in the join whose underlying Kafka topic has the smaller number of partitions.
   Let's call this stream/table "SMALLER", and the other side of the join, "LARGER".
2. Within your application, re-partition the data of "SMALLER".
   Ensure you use the same partitioner that is user for "LARGER".

    * If "SMALLER" is a KStream: KStream#repartition(Repartitioned.numberOfPartitions(...))
    * If "SMALLER" is a KTable: KTable#toStream#repartition(Repartitioned.numberOfPartitions(...).toTable())
3. Within your application, perform the join between the "LARGER" and the new stream/table.

## KStream-KStream join

KStream-KStream joins are always windowed joins, because otherwise the size of the internal state store used to perform
the join, e.g. a sliding window or "buffer", would grow indefinitely.

For KS-KS joins it's important to highlight that a new input record on one side will produce a join output for each
record on the other side (carthesian product), and there can be multiple such matching records in a given join window.

Join output records are effectively created as follows, leveraging the user-supplied ValueJoiner:

KeyValue<K, LV> leftRecord = ...;
KeyValue<K, RV> rightRecord = ...;
ValueJoiner<LV, RV, JV> joiner = ...;

KeyValue<K, JV> joinOutputRecord = KeyValue.pair(
    leftRecord.key, /* by definition, leftRecord.key == rightRecord.key */
    joiner.apply(leftRecord.value, rightRecord.value)
  );


### Inner Join: (KStream, KStream) → KStream

Performs an INNER JOIN of this stream with another stream.

Eventhough this operation is windowed, the joined stream will be of type KStream<K, V> rather than KStream<Windowed<K,V>>

Data in each stream must be co-partitioned.

Causes data re-partitioning if and only if the stream was marked for re-partitioning. If both are marked, both are
re-partitioned.

Detailed behavior:

* The join is key-based, i.e. with the join predicate leftRecord.key == rightRecord.key, 
  and window based, i.e. two input records are joined if and only if their timestamps are close to each other as
  defined by the user-supplied JoinWindows, i.e. windows defines an additional predicate over the record timestamps.

* The joins will be triggered under the following conditions whenever new input is received:

  1. Input records with a null key or a null value are ignored and do not trigger the join.
  2. Any other input from left or right with matching keys, trigger the join
  
* When a join is triggered, the user-supplied ValueJoiner will be called to produce join output records.

20:14:20 $ ll src/test/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_ks/O01_innerJoinTest.java 
-rw-rw-r-- 1 5713 nov 12 17:22 src/test/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_ks/O01_innerJoinTest.java

20:14:27 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_ks/O01_innerJoin.java 
-rw-rw-r-- 1 6233 nov 12 17:33 src/main/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_ks/O01_innerJoin.java

### leftJoin: (KStream, KStream) → KStream

Performs a left join on the left stream with the right stream.

Even though this operation is windowed, the joined stream will be of type KStream<K, V> instead of KStream<Windowed<K>, V>>

Data of both input streams must be co-partitioned.

Causes data re-partitioning of a stream if and only if the stream was marked for re-partitioning. If both are marked,
both are re-partitioned.

Detailed behavior:

* The join is key-based, i.e. with the join predicate leftRecord.key == rightRecord.key,
  and window-based, i.e. two input records are joined if and only if their timestams are close to each other as defined
  by the UserSupplied JoinWindows, i.e. the window defines an additional join predicate over the record timestamps.
  
* The join will be triggered under the conditions listed below whenever new input is received:

    1. Input records with a null key or a null value are ignored and do not trigger the join.
    2. Left record is the master of the trigger action:
      - Right records newly arrived; joins will wait until left record with matching key arrives.
      - Left records newly arrived trigger joins with existing right records with matching key that are queued to be
        matched
* For each input record on the left hand side that does not have any match on the right side, the ValueJoiner will be
  called with ValueJoiner#apply(leftRecord.value, null)
    

20:15:15 $ ll src/test/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_ks/O02_leftJoinTest.java 
-rw-rw-r-- 1 5705 nov 12 20:36 src/test/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_ks/O02_leftJoinTest.java

20:37:42 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_ks/O02_leftJoin.java 
-rw-rw-r-- 1 6388 nov 12 20:36 src/main/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_ks/O02_leftJoin.java

### outerJoin: (KStream, KStream) → KStream

Performs an outer join of the left stream with the right stream.

Eventhough this operation is windowed, the joined stream will be of type KStream<K, V> rather than KStream<Windowed<K>,V>

Left and rigth streams must be co-partitioned.

Causes data repartitioning of a stream if and only if the stream was marked before for re-partitioning. If both are
marked, both are re-partitioned.

Detailed behavior:

* The join is key-based, i.e. with the join predicate leftRecord.key == rightRecord.key
  and window-based, i.e. two input records are joined if and only if their timestamps are close to each other as
  defined by the user-supplied JoinWindows, i.e. the window defines an additional join predicate over the record timestamps.
* The join will be triggered under the conditions listed below whenever new input is received.

    1. Input records with a null key or a null value are ignored and do not trigger the join.
    2. Either left or right record are masters of the trigger action:
      - Left/right records newly arrived trigger joins with existing right/left records with matching key that are
        queued to be matched

* For each record on one side that does not have any match on the other side, the ValueJoiner will be called with
  ValueJoiner#apply(leftRecord.value, null) or ValueJoiner#apply(null, rightRecord.value), respectively.


20:41:59 $ ll src/test/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_ks/O03_outerJoinTest.java 
-rw-rw-r-- 1 5689 nov 13 06:52 src/test/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_ks/O03_outerJoinTest.java

20:41:51 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_ks/O03_outerJoin.java 
-rw-rw-r-- 1 6376 nov 13 06:52 src/main/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_ks/O03_outerJoin.java

## KTable-KTable join

KTable-KTable joins are always non windowed joins.

They are designed to be consistent with their counterparts in relational databases.

The changelog streams of both KTables are materialized into local State Stores to represent their latest snapshot.

The join result is a new KTable that represents the changelog stream of the join operation.

Join output records are effectively created as follows, leveraging the user-supplied ValueJoiner:

```sbtshell
KeyValue<K, LV> leftRecord = ...;
KeyValue<K, RV> rightRecord = ...;
ValueJoiner<LV, RV, JV> joiner = ...;

KeyValue<K, JV> joinOutputRecord = KeyValue.pair(
    leftRecord.key, // By definition, leftRecord.key == rightRecord.key
    joiner.apply(leftRecord.value, rightRecord.value)
);
```

### Inner Join: (KTable, KTable) → KTable

Performs an inner join of the left table with the right table.

The result is an ever-updating KTable that represents the "current" results of the join.

Left and right data must be co-partitioned.

Detailed behavior:

* The join is key-based, i.e. with the join predicate leftRecord.key == rightRecord.key
* The join will be triggered under the conditions below whenever the input is received. When it's triggered, the
  user-supplied ValueJoiner will be called to produce join output records.
  
  1. Input records with a null key are ignored and do not trigger the join.
  2. Input records with a null value are interpreted as tombstones for the corresponding key, which indicate the deletion
     of the key from the table.
     Tombstones do not trigger the join.
     When an input tombstone is received, then an output tombstone is forwarded directly to the output result KTable only
     if the corresponding key actually exists already in this output KTable.



07:22:12 $ ll src/test/java/io/confluent/examples/streams/streamdsl/stateful/joining/kt_kt/O01_innerJoinTest.java 
-rw-rw-r-- 1 4943 nov 13 15:31 src/test/java/io/confluent/examples/streams/streamdsl/stateful/joining/kt_kt/O01_innerJoinTest.java

07:22:00 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateful/joining/kt_kt/O01_innerJoin.java 
-rw-rw-r-- 1 6474 nov 13 08:50 src/main/java/io/confluent/examples/streams/streamdsl/stateful/joining/kt_kt/O01_innerJoin.java

### leftJoin: (KTable, KTable) → KTable

Performs a left join of the left table with the right table.

Both left and right data must be co-partitioned.

Detailed behavior:

* The join is key-based, i.e. with the join predicate leftRecord.key == rightRecord.key
* The join will be triggered under the conditions listed below whenever new input is received. When it is triggered,
  the user-supplied ValueJoiner will be called to produce join output records:
  
  1. Input records with a null key are ignored and do not trigger the join.
  2. Input records with a null value are interpreted as tombstones for the corresponding key, which indicate the
     deletion of the key from the table.
     Tombstones do not trigger the join.
     When an input tombstone is received, then an output tombstone is forwarded directly to the join result KTable if
     the corresponding key actually exist in the join result KTable.
* For each input record on the left side that doesn't have any match on the right side, the ValueJoiner will be called
  with ValueJoiner#apply(leftRecord.key, null)


15:31:54 $ ll src/test/java/io/confluent/examples/streams/streamdsl/stateful/joining/kt_kt/O02_leftJoinTest.java 
-rw-rw-r-- 1 4935 nov 14 09:27 src/test/java/io/confluent/examples/streams/streamdsl/stateful/joining/kt_kt/O02_leftJoinTest.java

16:10:31 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateful/joining/kt_kt/O02_leftJoin.java 
-rw-rw-r-- 1 6488 nov 14 09:18 src/main/java/io/confluent/examples/streams/streamdsl/stateful/joining/kt_kt/O02_leftJoin.java

## KStream-KTable join

KStream-KTable joins are always non-windowed joins.

They allow to perform table lookups agains a KTable, also known as changelog stream, upon receiving a new record from
the KStream.

An example use case would be to enrich a stream of user activities arriving in the KStream with the latest user profile
stored in the KTable.

Join output records are efficiently created as follows, leveraging the user-supplied ValueJoiner:

KeyValue<K, LV> leftRecord = ...;
KeyValue<K, RV> rightRecord = ...;
ValueJoiner<LV, RV, JV> joiner = ...;

KeyValue<K, JV> joinOutputRecord = KeyValue.pair(
    leftRecord.key, // by definition leftRecord.key == rightRecord.key
    joiner.apply(leftRecord.value, rightRecord.value)
);


**Records arriving order matters ...**

Records arriving to the left stream will only trigger the KS-KT if there are matching records in the right table.

I.e. if you do an insert in this order, you will get empty output join KSTream:


Timestamp |  KSTream record      KTable record      KSTream outout join       KSTream outout leftJoin       
----------|-------------------------------------------------------------------------------------------
000000001 |  (1, A)
000000002 |                      (1, a)             <empty output>            <empty output>

In your test this can be seen like like this. If you do this input ...:

        testInputTopic1.pipeKeyValueList(leftStreamInputValues);
        testInputTopic2.pipeKeyValueList(rightTableInputValues);


... you get this output:

java.lang.AssertionError: 
Expected: <{1=left=A, right=a}>
     but: was <{}>
Expected :<{1=left=A, right=a}>
Actual   :<{}> 

You can solve this problem in the test just changing the order of the pipe calls:

        // To trigger the join first the table must have matching elements
        testInputTopic2.pipeKeyValueList(rightTableInputValues);
        // After the table has elements, records arriving to the stream will start triggering joins with
        // the matching elements in the table
        testInputTopic1.pipeKeyValueList(leftStreamInputValues);


The test will then pass. You will see this happening after the change:

Timestamp  | KSTream record      KTable record      KSTream outout join       KSTream outout leftJoin       
-----------|------------------------------------------------------------------------------------------
000000001  |                     (1, a)
000000002  | (1, A)                                 Left=A, Right=a           Left=A, Right=a

### Inner Join: (KStream, KTable) → KStream

Performs an inner join on the left stream with the table, effectively doing a table lookup.

The input data from both left KStream and right KTable must be co-partitioned.

Causes data re-partitioning of the stream if and only if the stream was marked for re-partitioning.

Detailed behavior:

* The join is key-based, i.e. with the join predicate leftRecord.key == rigthRecord.key
* The join will be triggered under the conditions listed below whenever new input is received.
  When it's triggered, the user-supplied ValueJoiner will be called to produce join output records.
  
  1. Only input records from the stream in the left side trigger the join.
     Input records for the table in the right side update only the internal right-side join state.
  2. Input records for the stream with a null key or a null value are ignored and do not trigger the join.
  3. Input records for the table with a null value are interpreted as tombstones for the corresponding key, which
     indicate the deletion of the key from the table.
     Tombstones do not trigger the join.


06:52:42 $ ll src/test/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_kt/O01_innerJoinTest.java 
-rw-rw-r-- 1 4021 nov 16 06:47 src/test/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_kt/O01_innerJoinTest.java

06:52:55 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_kt/O01_innerJoin.java 
-rw-rw-r-- 1 6231 nov 16 06:51 src/main/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_kt/O01_innerJoin.java

### leftJoin: (KStream, KTable) → KStream

Performs a left join of the left stream with the right table, effectively doing a table lookup.

Input data in both stream and table must be co-partitioned.

Causes data re-partitioning of the stream if and only if the stream was marked for re-partitioning.

Detailed behavior:

* The join is key-based, i.e. with the join predicate being leftRecord.key == rightRecord.key
* The join will be triggered under the conditions listed below whenever new input is received. When it is triggered, the
  user-supplied ValueJoiner will be called to produce join output records.
  
  1. Only input records for the left side stream trigger the join.
     Input records for the right side table update only the internal right-side join state.
  2. Input records for the stream with a null key or a null value are ignored and do not trigger the join
  3. Input records for the table with a null value are interpreted as tombsotones for the corresponding key, which
     indicate the deletion of the key from the table.
       
     NOTE: Tombstones DO NOT trigger the join.

* For each input record on the left side stream that do not have any match on the right side table, the ValueJoiner will
  be called with ValueJoiner#apply(leftRecord.value, null)
  
09:55:50 $ ll src/test/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_kt/O02_leftJoinTest.java 
-rw-rw-r-- 1 4444 nov 16 11:28 src/test/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_kt/O02_leftJoinTest.java

12:40:52 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_kt/O02_leftJoin.java 
-rw-rw-r-- 1 6463 nov 16 11:03 src/main/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_kt/O02_leftJoin.java


## KStream-GlobalKTable Join

KSTream-GlobalKTable joins are always non-windowed joins.

They allow you to perform table lookups against a GlobalKTable, which is an entire changelog stream distributed in all
the partitions of a topic, which in fact are proportionally equal distributed between all the nodes of a Kafka cluster.

The join is triggered upon received a new record from the KStream.

An example use case would be "star queries" or "star joins", where you would enrich a KSTream of user activities with
the latest user profile information stored in the GlobalKTable plus further context information stored in further
GlobalKTables.

At a high level, KSTream-GlobalKTable joins are very similar to KSTream-KTable joins. However, global tables provide
you with much more flexibility at the some expense when compared to partitioned tables:

* They do not require data co-partitioning.
* They allow for efficient "star joins", i.e. joining a large-scale "facts" stream against "dimension" tables.
* They allow joining against foreign keys, i.e. you can lookup data in the table not just by the keys of records in the
  stream, but also by data in the record values.
* They make many use cases feasible where you must work on heavily skewed data and thus suffer from hot partitions.
* They are often more efficient than their partitioned KTable counterpart when you need to perform multiple joins in
  succession.
  
Join output records are effectively created as follows, leveraging the user-supplied ValueJoiner:

KeyValue<K, LV> leftRecord = ...;
KeyValue<K, RV> rightRecord = ...;
ValueJoiner<LV, RV, JV> joiner = ...;

KeyValue<K, JV> joinOutputRecord = KeyValue.pair(
    leftRecord.key,     // by definition, leftRecord.key == rightRecord.key
    joiner.apply(leftRecord.value, rightRecord.value)
);

### Inner Join: (KStream, GlobalKTable) → KStream

Performs an inner join of the left stream with the right global table, effectively doing a table lookup.

The GlobalKTable is fully bootstraped upon (re)start of a Kafka Streams instance, which means the table is fully
populated with all the data in the underlying topic that is available at the time of the startup.

The actual data processing begins only once the bootstrapping has completed.

Causes data re-partitioning of the left stream if and only if the left stream was marked for re-partitioning.

Detailed behavior:

* The join is indirectly key-based, i.e. with the join predicate 
  KeyValueMapper#apply(leftRecord.key, leftRecord.value) == rightRecord.key
* The join will be triggered under the conditions listed below whenever input is received.
  When it is triggered, the user-supplied ValueJoiner will be called to produce output records.
  
  1. Only input records for the left side stream trigger the join.
     Input records for tthe right side table update only the internal right-side join state.
  2. Input records for the left side stream with a null key or null value are ignored and do not trigger the join.
  3. Input records for the table with null value are interpreted as tombstones, which indicate the deletion of a record
     key from the table. 
     Tombstones do not trigger the join.
     
**USEFUL HELPER: Avro Schema From JSON Generator**

> |A|v|r|o| |S|c|h|e|m|a| |F|r|o|m| |J|S|O|N| |G|e|n|e|r|a|t|o|r|
>
> https://toolslick.com/generation/metadata/avro-schema-from-json

If you write this in the box

{
    "firstname": "aaa",
    "lastname": "bbb",
    "age": 30,
    "gender": "male",
    "vip": true,
    "nationality": "spanish"
}

You will get the corresponding AVSC codification:

**Note: change the "name" and the "namespace" accordingly in your code**

{
  "name": "Customer",
  "type": "record",
  "namespace": "io.confluent.examples.streams.streamsdsl.stateful.joining.ks_gkt.avro",
  "fields": [
    {
      "name": "firstname",
      "type": "string"
    },
    {
      "name": "lastname",
      "type": "string"
    },
    {
      "name": "age",
      "type": "int"
    },
    {
      "name": "gender",
      "type": "string"
    },
    {
      "name": "vip",
      "type": "boolean"
    },
    {
      "name": "nationality",
      "type": "string"
    }
  ]
}

17:17:29 $ ll src/main/resources/avro/io/confluent/examples/streams/streamsdsl.stateful.joining.ks_gkt/global-tables-schemas.avsc 
-rw-rw-r-- 1 1489 nov 17 06:24 src/main/resources/avro/io/confluent/examples/streams/streamsdsl.stateful.joining.ks_gkt/global-tables-schemas.avsc

Compile the code skipping tests:

$ mvn -DskipTests=true -Dcheckstyle.skip clean package

The new class set has been created from the new avro schema file:

07:01:14 $ ll target/generated-sources/io/confluent/examples/streams/streamsdsl/stateful/joining/ks_gkt/avro/
total 88
drwxrwxr-x 2  4096 nov 17 07:00 ./
drwxrwxr-x 3  4096 nov 17 07:00 ../
-rw-rw-r-- 1 17608 nov 17 07:00 Customer.java
-rw-rw-r-- 1 24863 nov 17 07:00 EnrichedOrder.java
-rw-rw-r-- 1 15327 nov 17 07:00 Order.java
-rw-rw-r-- 1 15395 nov 17 07:00 Product.java

17:17:22 $ ll src/test/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_gkt/O01_anyKindOfJoinTest.java 
-rw-rw-r-- 1 7042 nov 16 16:54 src/test/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_gkt/O01_anyKindOfJoinTest.java


17:17:37 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_gkt/O01_anyKindOfJoinDriver.java 
-rw-rw-r-- 1 8791 nov 16 17:15 src/main/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_gkt/O01_anyKindOfJoinDriver.java

17:17:52 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_gkt/O01_anyKindOfJoin.java 
-rw-rw-r-- 1 11634 nov 16 16:54 src/main/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_gkt/O01_anyKindOfJoin.java

### leftJoin: (KStream, GlobalKTable) → KStream

Performs a left join of the left stream with the global table, effectively doing a table lookup.

The GlobalKTable is fully bootstraped upon (re)start of a Kakfa Streams instance, which means the table is fully
populated with all the data in the underlying topic that is available at the time of the startup.

The actual data processing begins only once the bootstrapping has completed.

Causes data re-partitioning of the stream if and only if the stream was marked for re-partitioning.

Detailed behavior:

* The join is indirectly key-based, i.e. with the join predicate
  KeyValueMapper#apply(leftRecord.key, leftRecord.value) == rightRecord.key
  
* The join will be triggered under the conditions listed below whenever new input is received.
  When it is triggered, the user-supplied ValueJoiner will be called to produce join output records.
  
    1. Only input records for the left side stream trigger the join.
       Input records for the right side Global table update only the internal right-side join state.
    2. Input records for the left side stream with a null key or a null value are ignored and no not trigger the join.
    3. Input records for the global table with a null value are interpreted as tombstones, which indicate the deletion
       of a record key from the global table.
       Tombstones do not trigger the join.
       
* For each input record on the left hand side stream that doesn't have any match on the right hand side global table will
  be called with ValueJoiner#apply(leftRecord.value, null)


(SAME EXAMPLE THAN INNER JOIN)



## Stateful Windowing operations

Windowing lets you control how to group records that have the same key for stateful operations such aggregations or joins
into so-called windows.

Windows are tracked per record key.

NOTE: A related operation is grouping, which groups all records that have the same key to ensure that data is properly
      partitioned (keyed) for subsequent operations.
      Once grouped, windowing allows you to further sub-group the records of a key.
      
For example:

  . in join operations, a windowing State Store is used to store all the records received so far within the defined
    window boundary.
  . in aggregating operations, a windowing State Store is use to store the latest aggregation results per window.

Old records in the State Store are purged after the specified window retention period.
  
Kafka Streams guarantees to keep a window for at least this specifie time; the default value is one day and can be
changed via Materialized#WithRetention(...)

The DSL support these types of windows:

Window name 	        | Behavior 	       |  Short description                                                       
------------------------|------------------|--------------------------------------------------------------------------
Tumbling time window    | Time-based 	   | Fixed-size, non-overlapping, gap less window                             
Hoping time window      | Time-based 	   | Fixed-size, overlapping windows                                          
Sliging time window     | Time-based 	   | Fixed-size, overlapping windows that work in differences between record  
                        |                  | timestamps                                                               
Session window          | Session-based    | Dynamically-sized, non-overlapping, data-driven windows                  


### Example: custom time window

08:44:34 $ ll src/test/java/io/confluent/examples/streams/window/
-rw-rw-r-- 1 15016 oct 30 13:28 CustomWindowTest.java
-rw-rw-r-- 1  2452 oct 30 13:28 DailyTimeWindows.java

### Tumbling time windows

Tumbling windows are a special case of hoping time windows and are windows based on time intervals MEAURED IN MILLISECONDS.

They model fixed-size, non overlapping, gap-less windows.

A tumbling window is defined by a single property: the window's size.

A tumbling window is a hoping window whose window size is equal to it's advance interval.

Since tumbling window never overlap, a data record will belong to one and only one window.

Tumbling time windows are aligned to the epoch, with the lower interval bound being inclusive and the upper bound being exclusive.

"Aligned to the epoch" means that the first window starts at timestamp Zero.

For example, tumbling window with a size of 5000ms have predictable window boundaries:

(0;5000),(5000;10000),(10000;15000), ...


**A 5 MINUTE TUMBLING WINDOW**

Data records arriving in two streams                                                                 
                                                                                                                         
                    +-+  +-+      +-+  +-+  +-+   +-+         +-+   +-+   +-+   +-+               +-+   +-+   +-+   +-+   
 STREAM 1 --->      |X|  |·|      |X|  |X|  |·|   |X|         |·|   |X|   |·|   |X|               |·|   |X|   |X|   |·|   
                    +-+  +-+      +-+  +-+  +-+   +-+         +-+   +-+   +-+   +-+               +-+   +-+   +-+   +-+   
                                       +-+              +-+         +-+               +-+               +-+               +-+
 STREAM 2 --->                         |X|              |X|         |X|               |·|               |X|               |·|
                                       +-+              +-+         +-+               +-+               +-+               +-+
                  0                             5                             10                            15   
                  |                             |                             |                             |    
           -------|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|------->
                  |                             |                             |                             |    

Windows are created per record key
                    
                   +---------------------------+ +---------------------------+ +---------------------------+ +-----------
                   |+-+           +-+  +-+     | |+-+   +-+         +-+      | |+-+                     +-+| |+-+  
 RECORD KEY        ||X|           |X|  |X|     | ||X|   |X|         |X|      | ||X|                     |X|| ||X|  
    +-+            |+-+           +-+  +-+     | |+-+   +-+         +-+      | |+-+                     +-+| |+-+  
    |X|            |                   +-+     | |                  +-+      | |                        +-+| |     
    +-+            |                   |X|     | |                  |X|      | |                        |X|| |     
                   |                   +-+     | |                  +-+      | |                        +-+| |     
                   |                           | |                           | |                           | | 
                   |  0-5 min, key X, count 4  | | 5-10 min, key X, count 4  | | 10-15 min, key X, count 3 | |
                   +---------------------------+ +---------------------------+ +---------------------------+ +-----------
                           
                   +---------------------------+ +---------------------------+ +---------------------------+ +-----------
           +-+     |     +-+                +-+| |            +-+         +-+| |    +-+           +-+      | |      +-+   +-+ 
RECORD KEY |·|     |     |·|                |·|| |            |·|         |·|| |    |·|           |·|      | |      |·|   |·| 
           +-+     |     +-+                +-+| |            +-+         +-+| |    +-+           +-+      | |      +-+   +-+ 
                   |                           | |                           | |                           | | 
                   |  0-5 min, key ·, count 2  | | 5-10 min, key ·, count 2  | | 10-15 min, key ·, count 2 | |
                   +---------------------------+ +---------------------------+ +---------------------------+ +-----------

           
 
13:38:36 $ ll src/test/java/io/confluent/examples/streams/streamdsl/stateful/windowing/O01_tumblingWindowTest.java 
-rw-rw-r-- 1 11940 nov 18 13:31 src/test/java/io/confluent/examples/streams/streamdsl/stateful/windowing/O01_tumblingWindowTest.java

10:33:40 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateful/windowing/O01_tumblingWindow.java 
-rw-rw-r-- 1 6628 nov 18 13:09 src/main/java/io/confluent/examples/streams/streamdsl/stateful/windowing/O01_tumblingWindow.java

### Hopping time windows
 
Hopping time windows are windows based on time intervals.

They model fixed-size possibly overlapped windows.

A hopping window is defined by two properties: the window size and it's advance interval, aka "hop"

The advance interval specifies by how much a window moves forward relative to the previous one.

For example, you can configure a hopping window with a size 5 minutes and an advance time of 1 minute.

Since hopping window can overlap, and in general they do, a data record may belong to more than one such record.

NOTE: hopping windows vs sliding windows: hopping windows are sometimes called sliding windows in other stream processing
      tools. Kafka Streams follows the terminology in academic literature, where the semantics of sliding windows
      are different to hoping windows.
      
Hopping time windows are aligned to the epoch, with the lower interval bound being inclusive and the upper bound being exclusive.

"Aligned to the epoch" means that the first window starts at timestamp zero.

For example, hopping windows with a size of 5000ms and an advance interval, i.e. "hop", of 3000ms have predictable boundaries
[0;5000),[3000;8000),[6000;11000),[9000;14000), ...


**A 5 MINUTE HOPING WINDOW WITH A 1 MINUTE "HOP"**

Data records arriving in two streams                                                                 
                                                                                                                         
                    +-+  +-+      +-+  +-+  +-+   +-+         +-+   +-+   +-+   +-+               +-+   +-+   +-+   +-+   
 STREAM 1 --->      |X|  |·|      |X|  |X|  |·|   |X|         |·|   |X|   |·|   |X|               |·|   |X|   |X|   |·|   
                    +-+  +-+      +-+  +-+  +-+   +-+         +-+   +-+   +-+   +-+               +-+   +-+   +-+   +-+   
                                       +-+              +-+         +-+               +-+               +-+               +-+
 STREAM 2 --->                         |X|              |X|         |X|               |·|               |X|               |·|
                                       +-+              +-+         +-+               +-+               +-+               +-+
                  0                             5                             10                            15   
                  |                             |                             |                             |    
           -------|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|------->
                  |                             |                             |                             |    

Hoping windows are created per record key


----> hop 0        +---------------------------+ +---------------------------+ +---------------------------+ +-----------
                   |+-+           +-+  +-+     | |+-+   +-+         +-+      | |+-+                     +-+| |+-+  
 RECORD KEY        ||X|           |X|  |X|     | ||X|   |X|         |X|      | ||X|                     |X|| ||X|  
    +-+            |+-+           +-+  +-+     | |+-+   +-+         +-+      | |+-+                     +-+| |+-+  
    |X|            |                   +-+     | |                  +-+      | |                        +-+| |     
    +-+            |                   |X|     | |                  |X|      | |                        |X|| |     
                   |                   +-+     | |                  +-+      | |                        +-+| |     
                   |                           | |                           | |                           | | 
                   |  0-5 min, key X, count 4  | | 5-10 min, key X, count 4  | | 10-15 min, key X, count 3 | |
                   +---------------------------+ +---------------------------+ +---------------------------+ +-----------
                           
                   +---------------------------+ +---------------------------+ +---------------------------+ +-----------
           +-+     |     +-+                +-+| |            +-+         +-+| |    +-+           +-+      | |      +-+   +-+ 
RECORD KEY |·|     |     |·|                |·|| |            |·|         |·|| |    |·|           |·|      | |      |·|   |·| 
           +-+     |     +-+                +-+| |            +-+         +-+| |    +-+           +-+      | |      +-+   +-+ 
                   |                           | |                           | |                           | | 
                   |  0-5 min, key ·, count 2  | | 5-10 min, key ·, count 2  | | 10-15 min, key ·, count 2 | |
                   +---------------------------+ +---------------------------+ +---------------------------+ +-----------

----> hop 1
                  0                             5                             10                            15   
                  |                             |                             |                             |    
           -------|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|------->
                  |                             |                             |                             |    
                         +---------------------------+ +---------------------------+ +---------------------------+ +-----------
                         |        +-+  +-+        +-+| |+-+         +-+         +-+| |                  +-+   +-+| |
 RECORD KEY              |        |X|  |X|        |X|| ||X|         |X|         |X|| |                  |X|   |X|| |
    +-+                  |        +-+  +-+        +-+| |+-+         +-+         +-+| |                  +-+   +-+| |
    |X|                  |             +-+           | |            +-+            | |                  +-+      | |
    +-+                  |             |X|           | |            |X|            | |                  |X|      | |
                         |             +-+           | |            +-+            | |                  +-+      | |
                         |                           | |                           | |                           | | 
                         |1+(0-5) min, key X, count 4| |1+(5-10) min, key X,count 4| |1+(10-15) min,key X,count 3| |
                         +---------------------------+ +---------------------------+ +---------------------------+ +-----------
                           
                         +---------------------------+ +---------------------------+ +---------------------------+ +-----------
           +-+           |+-+                +-+     | |       +-+         +-+     | |+-+           +-+          | | +-+   +-+ 
RECORD KEY |·|           ||·|                |·|     | |       |·|         |·|     | ||·|           |·|          | | |·|   |·| 
           +-+           |+-+                +-+     | |       +-+         +-+     | |+-+           +-+          | | +-+   +-+ 
                         |                           | |                           | |                           | | 
                         |1+(0-5) min, key ·, count 2| |1+(5-10) min, key ·,count 2| |1+(10-15) min,key ·,count 2| |
                         +---------------------------+ +---------------------------+ +---------------------------+ +-----------

----> hop 2
                  0                             5                             10                            15   
                  |                             |                             |                             |    
           -------|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|------->
                  |                             |                             |                             |    
                               +---------------------------+ +---------------------------+ +---------------------------+ +-----------
                               | +-+  +-+        +-+   +-+ | |     +-+         +-+       | |           +-+   +-+       | |
 RECORD KEY                    | |X|  |X|        |X|   |X| | |     |X|         |X|       | |           |X|   |X|       | |
    +-+                        | +-+  +-+        +-+   +-+ | |     +-+         +-+       | |           +-+   +-+       | |
    |X|                        |      +-+                  | |     +-+                   | |           +-+             | |
    +-+                        |      |X|                  | |     |X|                   | |           |X|             | |
                               |      +-+                  | |     +-+                   | |           +-+             | |
                               |                           | |                           | |                           | |
                               |2+(0-5) min, key X, count 4| |2+(5-10) min, key X,count 4| |2+(10-15) min,key X,count 3| |
                               +---------------------------+ +---------------------------+ +---------------------------+ +-----------
                             
                               +---------------------------+ +---------------------------+ +---------------------------+ +-----------
           +-+                 |             +-+           | | +-+         +-+        +-+| |        +-+             +-+| | +-+ 
RECORD KEY |·|                 |             |·|           | | |·|         |·|        |·|| |        |·|             |·|| | |·| 
           +-+                 |             +-+           | | +-+         +-+        +-+| |        +-+             +-+| | +-+ 
                               |                           | |                           | |                           | | 
                               |2+(0-5) min, key ·, count 2| |2+(5-10) min, key ·,count 2| |2+(10-15) min,key ·,count 2| |
                               +---------------------------+ +---------------------------+ +---------------------------+ +-----------

----> hop 3

                o o o o o o o o o

13:38:48 $ ll src/test/java/io/confluent/examples/streams/streamdsl/stateful/windowing/O02_hoppingTimeWindowTest.java 
-rw-rw-r-- 1 11675 nov 18 15:17 src/test/java/io/confluent/examples/streams/streamdsl/stateful/windowing/O02_hoppingTimeWindowTest.java

 (6.0.0-post)
13:40:02 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateful/windowing/O02_hoppingTimeWindow.java 
-rw-rw-r-- 1 7004 nov 18 14:58 src/main/java/io/confluent/examples/streams/streamdsl/stateful/windowing/O02_hoppingTimeWindow.java


## Sliding time windows

Sliding time windows are actually quite different from hoping and tumbling windows.

In Kafka Streams, sliding windows are used only for join operations and can be specified through the JoinWindows class

A sliding window models a fixed-size window that slides continuously over the time axis; here, two data records are said to be
included in the same window if, in the case of simmetric windows, the difference of their timestamps is within the window size.

Thus, sliding windows are not aligned to the epoch, but to the data record timestamps.

In contrast to hoping windows, the lower and upper window time interval bounds of sliding windows are both inclusive.


20:14:20 $ ll src/test/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_ks/O01_innerJoinTest.java 
-rw-rw-r-- 1 5713 nov 12 17:22 src/test/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_ks/O01_innerJoinTest.java

20:14:27 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_ks/O01_innerJoin.java 
-rw-rw-r-- 1 6233 nov 12 17:33 src/main/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_ks/O01_innerJoin.java



20:15:15 $ ll src/test/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_ks/O02_leftJoinTest.java 
-rw-rw-r-- 1 5705 nov 12 20:36 src/test/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_ks/O02_leftJoinTest.java

20:37:42 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_ks/O02_leftJoin.java 
-rw-rw-r-- 1 6388 nov 12 20:36 src/main/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_ks/O02_leftJoin.java



20:41:59 $ ll src/test/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_ks/O03_outerJoinTest.java 
-rw-rw-r-- 1 5689 nov 13 06:52 src/test/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_ks/O03_outerJoinTest.java

20:41:51 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_ks/O03_outerJoin.java 
-rw-rw-r-- 1 6376 nov 13 06:52 src/main/java/io/confluent/examples/streams/streamdsl/stateful/joining/ks_ks/O03_outerJoin.java

## Session windows

Session windows are used to aggregate key-based events into so-called sessions; the process of wich is referred to as
sessionization.

Sessions represent a period of activity separated by a defined gap of inactivity, or "idleness".

Any events processed that fall within the inactivity gap of any existing sessions are merged into the existing sessions.

If an event falls outside of the session gap, then a new session will be created.

Session windows are different from the other window types in that:

* All windows are tracked independently across keys, e.g. windows of different keys typically have different start and
  end times.
* Their window sizes vary, even window for the same key typically have different sizes.

The prime area of application or use case is USER BEHAVIOR ANALYSIS.

Session based analysis can range for simple metrics, e.g. count of user visits on a news web site or social platform,
to more complex metrics, e.g. customer conversion funnel and event flows.


Given a session window of 5 minutes, here's what would happen if we had records for a key arriving late to a session window
(more than 5 minutes latency): new arriving records for the key in the expired session window are included in a new session
window:
                                                                                
                    +-+   +-+                           +-+   +-+                           +-+         +-+  
                    |X|   |·|                           |X|   |·|                           |·|         |X|  
                    +-+   +-+                           +-+   +-+                           +-+         +-+  
                  0                             5                             10                            15   
                  |                             |                             |                             |    
           -------|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|------->
                  |                             |                             |                             |    

Session windows are created per record key, not grouped because the inactivity for each key exceeds the session window of 5 minutes:


                   +---+                               +---+                                           +---+
                   |+-+|                               |+-+|                                           |+-+|
                   ||X||                               ||X||                                           ||X||
                   |+-+|                               |+-+|                                           |+-+|
                   +---+                               +---+                                           +---+
                         +---+                               +---+                         +---+
                         |+-+|                               |+-+|                         |+-+|
                         ||·||                               ||·||                         ||·||
                         |+-+|                               |+-+|                         |+-+|
                         +---+                               +---+                         +---+



Again, given a session window of 5 minutes, here's what would happen if we had records for a key arriving on time to a session window
(less than 5 minutes latency): new arriving records for the key in the active session window are included on it:
                                                                                
                    +-+   +-+               +-+              +-+    +-+         +-+         +-+  
                    |X|   |·|               |X|              |X|    |·|         |·|         |X|  
                    +-+   +-+               +-+              +-+    +-+         +-+         +-+  
                  0                             5                             10                            15   
                  |                             |                             |                             |    
           -------|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|------->
                  |                             |                             |                             |    

Session windows are created per record key, not grouped because the inactivity for each key exceeds the session window of 5 minutes:


                   +---------------------------------------------------------------------------+
Session Window 1   |+-+                     +-+               +-+                           +-+|
   for key X       ||X|                     |X|               |X|                           |X||
                   |+-+                     +-+               +-+                           +-+|
                   +---------------------------------------------------------------------------+
                         +---+                                     +----------------+
     Session Window 1    |+-+|                  Session Window 2   |+-+          +-+|
        for key ·        ||·||                     for key ·       ||·|          |·||
                         |+-+|                                     |+-+          +-+|
                         +---+                                     +----------------+
                                                                               
15:18:09 $ ll src/test/java/io/confluent/examples/streams/streamdsl/stateful/windowing/O04_sessionWindowTest.java 
-rw-rw-r-- 1 12829 nov 19 08:32 src/test/java/io/confluent/examples/streams/streamdsl/stateful/windowing/O04_sessionWindowTest.java

08:32:30 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateful/windowing/O04_sessionWindow.java 
-rw-rw-r-- 1 6641 nov 18 16:55 src/main/java/io/confluent/examples/streams/streamdsl/stateful/windowing/O04_sessionWindow.java


## Window final results

In Kafka Streams, windowed computations update their results continuously.

As new data arrives for a window, freshly computed results are emitted downstream.

For many application this is ideal, since fresh results are always available, and Kafka Streams is designated to
make programming continuous computation seamless.

However, some applications need to take action ONLY on the final result of a windowed computation.

Common examples of this are:

 . sending alerts on threshold trespassed or 
 . delivering results to a system that doesn't support updates
 
Suppose that you have an hourly windowed count events per user.

If you want to send an alert when a user has less than three events in an hour, you have a real challenge because all
user would match this condition at first until they accrue enougth events.

So you can't simply send an alert when someone matches the condition; instead, you have to wait until you know you won't
see any more events for that particular window, and then send the alert.

Kafka Stream offers a clean way to define this logic: after defining your windowed computation, you can supress the
intermediate results, emitting the final count for each user when the window is closed 


For example:

KGroupedStream<UserId, Event> grouped = ...;
grouped
    .windowedBy(TimeWindows.of(Duration.ofHours(1)).grace(Duration.ofMinutes(10)))
    .count()
    .suppress(Suppressed.untilWindowCloses(unbounded()))
    .filter((windowedUserId, count) -> count < 3)
    .toStream()
    .foreach((windowedUserId, count) -> sendAlert(windowedUserId.window(), windowedUserId.key(), count));

Copy

The key parts of this program are:

grace(Duration.ofMinutes(10))
    This allows us to bound the unorder (or delay) of events the window will accept. For example, the 09:00 to 10:00 window
    will accept out-of-order records until 10:10, at which point, the window is closed.
    
.suppress(Suppressed.untilWindowCloses(...))
    This configures the suppression operator to emit nothing for a window until it closes, and then emit the final result.
    For example, if user U gets 10 events between 09:00 and 10:10, the filter downstream of the suppression will get no events
    for the windowed key @09:00-10:00 until 10:10, and then it will get exactly one event with the value 10. This is the final
    result of the windowed count.
    
unbounded()
    This configures the buffer used for storing events until their windows close. Production code is able to put a cap on
    the amount of memory to use for the buffer, but this simple example creates a buffer with no upper bound.

One thing to note is that suppression is just like any other Kafka Streams operator, so you can build a topology with two branches
emerging from the count, one suppressed, and one not, or even multiple differently configured suppressions.

This allows you to apply suppressions where they are needed and otherwise rely on the default continuous update behavior.

For more detailed information, see the JavaDoc on the Suppressed config object and KIP-328 (https://cwiki.apache.org/confluence/x/sQU0BQ).


**A 5 MINUTE + 10 SECONDS GRACE PERIOD WINDOW ALERTING WHEN ARRIVED >3 RECORDS FOR A KEY**

Data records arriving in two streams                                                                 
                                                                                                                         
                    +-+  +-+      +-+  +-+  +-+   +-+         +-+   +-+   +-+   +-+               +-+   +-+   +-+   +-+   
 STREAM 1 --->      |X|  |·|      |X|  |·|  |·|   |X|         |·|   |X|   |·|   |X|               |·|   |X|   |X|   |·|   
                    +-+  +-+      +-+  +-+  +-+   +-+         +-+   +-+   +-+   +-+               +-+   +-+   +-+   +-+   
                                       +-+              +-+         +-+               +-+               +-+               +-+
 STREAM 2 --->                         |.|              |X|         |X|               |·|               |X|               |·|
                                       +-+              +-+         +-+               +-+               +-+               +-+
                  0                             5                             10                            15   
                  |                             |                             |                             |    
           -------|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|------->
                                              |                                                           |                                           
                                              |                                                           |
                                              |                                                           |
                                              |      +-+                                                  |      +-+
                                              V      |·|                                                  V      |X|
                                              Alert! +-+ count is > 3                                     Alert! +-+ count is > 3 



08:32:51 $ ll src/test/java/io/confluent/examples/streams/streamdsl/stateful/windowing/O05_windowFinalResults_ThresholdReachedTest.java 
-rw-rw-r-- 1 10219 nov 19 11:07 src/test/java/io/confluent/examples/streams/streamdsl/stateful/windowing/O05_windowFinalResults_ThresholdReachedTest.java

10:46:04 $ ll src/main/java/io/confluent/examples/streams/streamdsl/stateful/windowing/O05_windowFinalResults_ThresholdReached.java 
-rw-rw-r-- 1 7828 nov 19 10:45 src/main/java/io/confluent/examples/streams/streamdsl/stateful/windowing/O05_windowFinalResults_ThresholdReached.java


### Example: custom time window

08:44:34 $ ll src/test/java/io/confluent/examples/streams/window/
-rw-rw-r-- 1 15016 oct 30 13:28 CustomWindowTest.java
-rw-rw-r-- 1  2452 oct 30 13:28 DailyTimeWindows.java

In addition to using the windows implementations provided with the Kafka Streams client library, you can extend the Java Windows abstrac
class to create custom time windows to suit your use cases.

To view a custom implementation of a daily window starting every day at 6pm, refer to streams/window example.

The example also shows a potential problem in dealing with time zones that have daylight savings time.

# Applying processors and transformers (Processor API integration)

Beyond the aforementioned stateless and stateful transformations, we may also leverage the Processor API from the DSL.

There are a number of scenarios where this may be helpful:

* Customization: we need to implement special customized logic that is not or not yet available in DSL.

* Combination of easy-of-use with full flexibility where it is needed: eventhough we generally prefer to use the
  expresiveness of DSL, there are certain steps in our processing that may require more flexibility and tinkering than
  the DSL provides.
  For example, only the Processor API provides access to record's metadata such as topic, partition and offset information.
  However, we don't want to switch completely to the Processor API just because of that.
  
* Migrating from other tools: we are migrating from other stream processing technologies that provide an imperative API,
  and migrating some of our legacy code to the Processor API was faster and/or easier than to migrate completely to the
  DSL right away.
  
## process: KStream -> void

Terminal operation thar returns void: applies a Processor to each record.

.process() allows us to leverage the Processor API from the DSL.

This is essentially equivalent to adding the Processor via Topology#addProcessor() to our processor topology.

Process all records in this stream, one record at time, by adding a Processor provided by the given ProcessorSupplier.

Attaching a State Store makes this a Stateful record-by-record operation, cf. foreach(ForeachAction)

Not attaching a Satate Store is similar to the Stateless foreach(ForeachAction), but additionally allows access to the
ProcessorContext and record metadata.

Essentially, mixing the Processor API into the DSL provides the DSL all the functionality of the PAPI.

Furthermore, via the Punctuator.punctuate(long), the processing progress can be observed and additional periodic actions
can be performed.

Even if any upstream operation was key-changing, no auto-repartition is triggered. If repartitioning is required, a call
to repartition() should be performed before process().

In order for the processor to use State Stores, the stores must be added to the Topology and connected to the processor
using at least one or two strategies, thought it's not required to connect Global State Stores as read-only to them is
available by default:

1) the first strategy is to manually add the StoreBuilder via Topology.addStateStore(StoreBuilder, String, ...), and
   specify the store names via stateStoreNames so they will be connected to the processor.
   
   Recommended code for first strategy:
   
           KStream outputStream = sourceStream.processor(new ProcessorSupplier() {
            public Processor get() {
                return new EmailMetricThresholdAlert_Processor();
            }
        }, "myProcessorState");
    
    Alternative code:
    
        filteredStream.process(() -> new EmailMetricThresholdAlert_Processor(), stateStoreName);
   
14:15:48 $ ll src/test/java/io/confluent/examples/streams/streamdsl/processorapi/process/O01_processStrategy1ImplementProcessorTest.java 
-rw-rw-r-- 1 4078 nov 20 14:40 src/test/java/io/confluent/examples/streams/streamdsl/processorapi/process/O01_processStrategy1ImplementProcessorTest.java

14:16:50 $ ll src/main/java/io/confluent/examples/streams/streamdsl/processorapi/process/O01_processStrategy1_ImplementProcessor.java 
-rw-rw-r-- 1 8333 nov 20 14:40 src/main/java/io/confluent/examples/streams/streamdsl/processorapi/process/O01_processStrategy1_ImplementProcessor.java

14:53:13 $ ll src/main/java/io/confluent/examples/streams/streamdsl/processorapi/process/EmailMetricThresholdAlert_Processor.java 
-rw-rw-r-- 1 3532 nov 20 14:40 src/main/java/io/confluent/examples/streams/streamdsl/processorapi/process/EmailMetricThresholdAlert_Processor.java   

2) the second strategy is for the given ProcessorSupplier to implement ConnectedStoreProvider.stores(), which provides
   the StoreBuilders to be automatically added to the Topology and connected to the processor.


14:52:42 $ ll src/test/java/io/confluent/examples/streams/streamdsl/processorapi/process/O02_processStrategy2ImplementProcessorSupplierTest.java 
-rw-rw-r-- 1 4134 nov 20 14:41 src/test/java/io/confluent/examples/streams/streamdsl/processorapi/process/O02_processStrategy2ImplementProcessorSupplierTest.java

14:54:23 $ ll src/main/java/io/confluent/examples/streams/streamdsl/processorapi/process/O02_processStrategy2_ImplementProcessorSupplier.java 
-rw-rw-r-- 1 7739 nov 20 14:52 src/main/java/io/confluent/examples/streams/streamdsl/processorapi/process/O02_processStrategy2_ImplementProcessorSupplier.java


14:53:36 $ ll src/main/java/io/confluent/examples/streams/streamdsl/processorapi/process/EmailMetricThresholdAlert_ProcessorSupplier.java 
-rw-rw-r-- 1 1306 nov 20 14:51 src/main/java/io/confluent/examples/streams/streamdsl/processorapi/process/EmailMetricThresholdAlert_ProcessorSupplier.java

These examples have been implemented using the information provided here:

https://docs.confluent.io/current/streams/javadocs/org/apache/kafka/streams/kstream/KStream.html#process-org.apache.kafka.streams.processor.ProcessorSupplier-java.lang.String...-


We also have some examples here:


tsb-onboarding-otp-verification/src/main/java/com/tsb/onboarding/processor/OtpVerificationProcessor.java:        
```sbtshell
    private void process(KStream<String, RestProxyEventRequest> userInputStream,
                                         GlobalKTable<String, GenericToken> otpTable) {
    ...
    }

    this.process(userInputStream, otpTable);
```


## transform: KStream -> KStream


An example is available here:

https://docs.confluent.io/current/streams/javadocs/org/apache/kafka/streams/kstream/KStream.html#transform-org.apache.kafka.streams.kstream.TransformerSupplier-java.lang.String...-

Applies a transformer to each record.

transform() allows us to leverage the Processor API from the DSL.

Each input record is transformed into zero, one or more output records, similarly to stateless flatMap.

The Transformer must return null for zero output.

We can modify the record's key and value, including their types.

Marks the stream for data re-partitioning: applying a grouping or a join after transform() will result
in re-partitioning of the records.

If possible use transformValues instead, which will not cause data re-partitioning.

transform() is essentially equivalent to adding the Transformer via Topology#addProcessor() to the Topology.

A Transformer provided by the given TransformSupplier is applied to each input record and returns zero or one record.

Thus, an input record <K, V> can be transformed into an output record <K', V'>
 
Attaching a State Store makes this a Stateful record-by-record operation, cf. map().

If we choose not attaching a State Store, this operation then is similar to the Stateless map(), with the additional 
capability to access the ProcessorContext and record metadata.

This is essentially mixing the Processor API into the DSL, and provides all the functionallity of the PAPI.

Furthermore, via Punctuator#punctuate(), the processing progress can be observed and additional periodic actions can
be performed.

In order for the transformer to use the State Stores, the stores must be added to the topology and connected to the
transformer using at least one of two strategies, though it's not required to connect global state stores; read-only
access to global state stores is available by default:

1) The first strategy is to manually add the StoreBuilders via Topology.addStateStore(StoreBuilder, String, ...) and
specify the store names via stateStoreNames so they will be connected to the transformer.

14:54:43 $ ll src/main/java/io/confluent/examples/streams/streamdsl/processorapi/transform/

-rw-rw-r-- 1 1815 nov 22 13:45 EmailMetricThresholdAlert_Transformer.java
-rw-rw-r-- 1 8113 nov 22 13:59 O01_transformStrategy1_ImplementTransformer.java

14:00:44 $ ll src/test/java/io/confluent/examples/streams/streamdsl/processorapi/transform/O01_transformStrategy1ImplementTransformerTest.java 
-rw-rw-r-- 1 4110 nov 22 13:59 src/test/java/io/confluent/examples/streams/streamdsl/processorapi/transform/O01_transformStrategy1ImplementTransformerTest.java


2) The second strategy is for the given TransformerSupplier to implement ConnectedStoreProvider.stores(), which provides
   the StoreBuilders to be automatically added to the topology and connected to the transformer.


14:02:16 $ ll src/test/java/io/confluent/examples/streams/streamdsl/processorapi/transform/O02_transformStrategy2ImplementTransformerTest.java 
-rw-rw-r-- 1 4110 nov 22 13:40 src/test/java/io/confluent/examples/streams/streamdsl/processorapi/transform/O02_transformStrategy2ImplementTransformerTest.java

14:54:43 $ ll src/main/java/io/confluent/examples/streams/streamdsl/processorapi/transform/

-rw-rw-r-- 1 1815 nov 22 13:45 EmailMetricThresholdAlert_Transformer.java
-rw-rw-r-- 1 1448 nov 22 13:32 EmailMetricThresholdAlert_TransformerSupplier.java
-rw-rw-r-- 1 7612 nov 22 13:42 O02_transformStrategy2_ImplementTransformer.java

Also examples here:

14:55:01 $ ll src/test/java/io/confluent/examples/streams/streamdsl/processorapi/transform/

-rw-rw-r-- 1 6783 nov 20 15:56 MixAndMatchLambdaIntegrationTest.java
-rw-rw-r-- 1 7633 nov 20 14:20 StreamDSLAndProcessorAPIExampleTest.java

```sbtshell
    private void process(KStream<String, RestProxyEventRequest> userInputStream,
                                         GlobalKTable<String, GenericToken> otpTable) {
    ...
    }

    this.process(userInputStream, otpTable);
```

## transformValues: KStream -> KStream

Applies a ValueTransformer to each record, while retaining the key of the original record.

transformValues() allows you to leverage the Processor API from the DSL.

Each input record is transformed into exactly one output record, zero output records or multiple output records are not possible.

The ValueTransformer may return null as the new value for a record.

transformValues() is preferable to transform() because it will not cause data re-partitioning.

It is also possible to have read-only access to the input record key if you use ValueTransformerWithKey instead. This is provided via
ValueTransformerWithKeySupplier.

transformValues() is essentially equivalent to adding the ValueTransformer via Topology#addProcessor() to the Topology.

https://docs.confluent.io/platform/current/streams/javadocs/org/apache/kafka/streams/kstream/KStream.html#transformValues-org.apache.kafka.streams.kstream.ValueTransformerSupplier-org.apache.kafka.streams.kstream.Named-java.lang.String...-

14:02:42 $ ll src/test/java/io/confluent/examples/streams/streamdsl/processorapi/transformvalues/
total 24
-rw-rw-r-- 1 4200 nov 22 17:44 O01_transformValuesStrategy1ImplementTransformerTest.java
-rw-rw-r-- 1 4256 nov 22 18:14 O02_transformValuesStrategy2ImplementTransformerSupplierTest.java


14:00:15 $ ll src/main/java/io/confluent/examples/streams/streamdsl/processorapi/transformvalues/
total 28
-rw-rw-r-- 1 8570 nov 22 17:49 O01_transformValuesStrategy1_ImplementTransformer.java
-rw-rw-r-- 1 7868 nov 22 18:15 O02_transformValuesStrategy2_ImplementTransformerSupplier.java


07:11:48 $ ll src/main/java/io/confluent/examples/streams/streamdsl/processorapi/transformvalueswithkey/
total 28
-rw-rw-r-- 1 8602 nov 23 07:09 O01_transformValuesStrategy1_ImplementTransformerWithKey.java
-rw-rw-r-- 1 7900 nov 23 07:09 O02_transformValuesStrategy2_ImplementTransformerWithKeySupplier.java

07:07:15 $ ll src/test/java/io/confluent/examples/streams/streamdsl/processorapi/transformvalueswithkey/
total 24
-rw-rw-r-- 1 4256 nov 23 07:09 O01_transformValuesStrategy1ImplementTransformerWithKeyTest.java
-rw-rw-r-- 1 4312 nov 23 07:09 O02_transformValuesStrategy2ImplementTransformerWithKeySupplierTest.java


18:17:46 $ tree src/main/java/io/confluent/examples/streams/streamdsl/processorapi/utils/
src/main/java/io/confluent/examples/streams/streamdsl/processorapi/utils/
├── processors
│   ├── EmailMetricThresholdAlert_Processor.java
│   └── EmailMetricThresholdAlert_ProcessorSupplier.java
└── transformers
    ├── EmailMetricThresholdAlert_Transformer.java
    ├── EmailMetricThresholdAlert_TransformerSupplier.java
    ├── EmailMetricThresholdAlert_ValueTransformer.java
    ├── EmailMetricThresholdAlert_ValueTransformerSupplier.java
    ├── EmailMetricThresholdAlert_ValueTransformerWithKey.java
    └── EmailMetricThresholdAlert_ValueTransformerWithKeySupplier.java

# Record caches in the DSL

https://docs.confluent.io/platform/current/streams/developer-guide/memory-mgmt.html#record-caches-in-the-dsl

We can specify the total RAM memory size used for internal caching and compacting records for an instance of the
processing topology.

This caching happens before the records are written to State Stores or forwarded downstream to other nodes.

It is leveraged by the following KTable instances:

* source KTable
* aggregation KTable

For such KTable instances, the record cache is used for:

* Internal caching and compacting of output records before they are written by the underlying stateful processor node
  to:
  
  . it's internal State Stores.
  . any of it's downstream processor nodes.


For example, if we used this bit of code with the following records <"A", 1>, <"A", 20>, <"A", 300>:

```sbtshell
        // Key: word, value: count
        KStream<String, Long> wordCounts = streamsBuilder.stream(inputTopic,
                Consumed.with(Serdes.String(), Serdes.Long()));

        // Group the stream
        KGroupedStream<String, Long> groupedStream =
                wordCounts.groupByKey(Grouped.with(Serdes.String(), Serdes.Long()));

        KTable<Windowed<String>, Long> countStream =
            groupedStream.count();

        countStream.toStream().print(Printed.toFile("/tmp/outputrecords.txt"));

        countStream.toStream().to("output-topic");
```

We would have different records in the output topic depending if we used cached or not:

* Without caching would be this sequence of records: <A, (1, null)>, <A, (21, 1)>, <A, (321, 21)>

    06:24:56 $ ll src/test/java/io/confluent/examples/streams/streamdsl/cache/
    -rw-rw-r-- 1  5081 nov 25 06:56 O01_aggregationNotCachedTest.java

    06:57:51 $ ll src/main/java/io/confluent/examples/streams/streamdsl/cache/
    -rw-rw-r-- 1  7621 nov 25 06:22 O01_aggregationNotCached.java

* With caching, the sequence should be this:          <A, (321, null)>

    06:24:56 $ ll src/test/java/io/confluent/examples/streams/streamdsl/cache/
    -rw-rw-r-- 1  5151 nov 25 06:24 O02_aggregationCachedTest.java

    06:57:51 $ ll src/main/java/io/confluent/examples/streams/streamdsl/cache/
    -rw-rw-r-- 1  8415 nov 25 06:57 O02_aggregationCached.java

The cache size is specified through the cache.max.bytes.buffering parameter, which is a global setting
per processing topology:

```
    // Enable record cache of size 10 MB.
    Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);
```

This parameter controls the number of bytes allocated for caching.

Specifically for a processor Topology instance with T threads and C bytes allocated for caching, each
thread will have an even C/T bytes to construct it's own cache and use it as it sees fit among it's tasks.

This means there are as many caches as there are threads, but no sharing caches accross threads happens.

The basic API for the cache is made of put() and get() calls.

Records are evicted using simple LRU scheme after the cache size is reached.

The first time a keyed record R1 = <K1, V1> finishes processing at a node, it is marked as dirty in the cache.

Any other keyed record R2 = <K1, V2> with the same key K1 that is processed on that node during that time will
overwrite <K1, V1>, this is referred to as "being compacted".

This has the same effect as Kafka's log compaction, but happens earlier, while the records are still in memory
and within your client side application rather than on the server-side Kafka broker.

After flushing, R2 is forwarded to the next processing node and then written to the local State Store.

The semantics of caching is that data is flushed to the State Store and forwarded to the next downstream processor
node whenever the earliest of commit.interval.ms or cache.max.bytes.buffering are global parameters.

As such, it is not possible to specify different parameters for individual nodes.

Here are example settings for both parameters based on desired scenarios.

  * With default settings, caching is enabled in Kafka Streams and RocksDB.

  * To turn off caching the cache size can be set to zero:

```   
    // Disable record cache
    Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
```    

  * To enable caching but still have an upper bound on how long records will be cached,
    you can set the commit interval. In this example, it is set to 1000 milliseconds:

```    
    Properties streamsConfiguration = new Properties();
    // Enable record cache of size 10 MB.
    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);
    // Set commit interval to 1 second.
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
```

## Controlling KTable emit rate

https://cwiki.apache.org/confluence/display/KAFKA/KIP-328%3A+Ability+to+suppress+updates+for+KTables

A KTable is logically a continuously updated table.

These updates make their way to downsteam operators whenever new data is available, ensuring that the whole computation is
as fresh as possible.

Most programs describe a series of logical transformations, and their update rate is not a factor in the program behavior.

In these cases, the rate of update is a performance concern, which best addressed directly via the relevant configurations.

However, for some applications, the rate of update itself is an important semantic property.

Rather than achieving this as a side-effect of the record caches, we can directly impose a rate limit via the
KTable#supress operator.


For example:

06:06:13 $ ll src/test/java/io/confluent/examples/streams/streamdsl/ktableemitrate/O01_windowedByCountWithKtableEmitRateTest.java 
-rw-rw-r-- 1 6066 nov 24 06:07 src/test/java/io/confluent/examples/streams/streamdsl/ktableemitrate/O01_windowedByCountWithKtableEmitRateTest.java

06:03:29 $ ll src/main/java/io/confluent/examples/streams/streamdsl/ktableemitrate/O01_windowedByCountWithKTableEmitRate.java 
-rw-rw-r-- 1 7489 nov 23 07:44 src/main/java/io/confluent/examples/streams/streamdsl/ktableemitrate/O01_windowedByCountWithKTableEmitRate.java

```
        KTable<Windowed<String>, Long> timeWindowedCountStream =
            groupedStream
              .windowedBy(TimeWindows.of(Duration.ofMinutes(5)))
              .count()
              // each key is updated no more than once every 5 minutes in stream time,
              // not wall-clock time.
              .suppress(Suppressed.untilTimeLimit(Duration.ofMinutes(5),
                                                  Suppressed.BufferConfig
                                                          // maximum amount of memory to use
                                                          // for this buffer
                                                          .maxBytes(1_000_000L)
                                                          // maximum number of records to use
                                                          // for this buffer
                                                          .withMaxRecords(10000L)
                                                          // emit the oldest records before their 
                                                          // 5-minute time limit to bring the
                                                          //buffer back down to size
                                                          .emitEarlyWhenFull()
                                                )
                        );
```                        

This configuration ensures that each key is updated no more than once every 5 minutes in stream time, not wall-clock
time.

Note that the latest state for each key has to be buffered in memory for that 5 minute period.

We have the option to control the maximum amount of memory to use for this buffer, in our example 1MB.
.maxBytes(1_000_000L)

There's also an option to impose a limit in terms of number of records, or to leave both limits unspecified too.
.maxRecords(10000L)

Additionally it is possible to choose what happens if the buffer fills up.

This example takes a relaxed approach and just emits the oldest records before their 5-minute time limit to bring the
buffer back down to size (.emitEarlyWhenFull()).

Alternatively, you can choose to stop processing and shut the application down (.shutDownWhenFull()).

This may seem extreme, but it gives you a guarantee that the 5-minutes time limit will be absolutely enforced.

After the application shuts down, you can allocate more memory for the buffer and resume processing.

Emitting early is preferable for most applications than shutting down. 

## Writing back streams to Kafka

Any streams and tables may be continuously written back to a Kafka topic.

The output data might be re-partitioned on it's way to Kafka, depending on the situation.


### to: KStream -> void

Terminal operation. Write the records to Kafka topic(s).

When to provide serdes explicitly?:

 * If we don't specify SerDes explicitly, the default SerDes from the configuration are used.
 * We must specify SerDes explicitly via the Produced class if the key and/or value types of
   the KStream do not match the configured default SerDes.
 * "Kafka Streams Data Types and Serialization below" shows detailed information about
   configuring default SerDes, available SerDes and implementing our own custom SerDes.

A variant of `to` exists that enables us to specify how the data is produced by using a Produced
instance to specify, for example, a StreamPartitioner that gives us control over how output records
are distributed across the partitions of the output topic.

Another variant of `to` enables us to dinamically choose which topic to send to for each record via a
TopicNameExtractor instance.

`to` causes re-partitioning if any of the following conditions is true:

* if the output topic has a different number or partitions than the stream/table.
* if the KStream was marked for re-partitioning.
* if we provide a custom StreamPartitioner to explicitly how to distribute the output records across
  the partitions of the output topic.
* if the key of an otuput record is null.

When we want to write to systems other than Kafka: besides writing the data back to Kafka, we can also apply
a custom processor (see the section before with `process` and `transform` examples) as a stream sink
at the end of the processing to, for example, write to external databases (ScyllaDB for example).

Please, notice that doing so is not a recommended pattern by Confluent engineers, and they strongly suggest
to use the Kafka Connect API instead.

However, if we do not have a choice other than using a Custom Processor, we need to be aware that it is OUR
responsibility to guarantee message delivery semantics when talking to such external systems, e.g. retry
on delivery failure or prevent message duplication. 

In other words, our Custom Processor must include our own custom development to guarantee them ... not an easy
task, indeed. 

https://kafka-tutorials.confluent.io/dynamic-output-topic/kstreams.html

7:47:56 $ ll src/main/java/io/confluent/examples/streams/streamdsl/fromstreamtotopic/
-rw-rw-r-- 1  7031 nov 28 06:47 O1_UseCustomPartitionerDinamicallyChooseDestTopic.java

18:27:34 $ ll src/test/java/io/confluent/examples/streams/streamdsl/fromstreamtotopic/
-rw-rw-r-- 1  4920 nov 28 06:42 O1_UseCustomPartitionerDinamicallyChosenDestTopicTest.java

