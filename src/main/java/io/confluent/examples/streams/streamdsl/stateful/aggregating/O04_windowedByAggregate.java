package io.confluent.examples.streams.streamdsl.stateful.aggregating;

import io.confluent.common.utils.TestUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.Properties;

/**
 * Demonstrating the KStream DSL `windowdBy` and `aggregate` methods applied together
 *
 **/

public class O04_windowedByAggregate {
    static final String inputTopic = "input";
    static final String timeWindowedAggStreamOutputTopic = "time-windowed-aggregated-stream-output";
    static final String timeWindowedAggStreamFromCogroupOutTopic = "time-windowed-aggr-stream-from-cogroup-output";
    static final String sessionizedAggStreamOutputTopic = "sessionized-aggr-stream-output";
    static final String sessionizedAggStreamFromCogroupOutTopic = "sessionized-aggr-stream-from-cogroup-output";
    public static void main(String[] args) {

        final String bootstrapServers = args.length > 0? args[0]: "localhost:9092";

        // Configure the Streams application
        final Properties streamsConfiguration = getStreamsConfiguration(bootstrapServers);

        // Define the processing topology of the Streaming application
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        createStream(streamsBuilder);

        final Topology topology = streamsBuilder.build();

        System.out.println("\n||||||||||||||||||\n" + topology.describe() +
                "You can see it in http://localhost:8080/kafka-streams-viz/\n" +
                "For that you must run ~/Downloads/apache-tomcat-9.0.39/bin/catalina.sh start\n" +
                "You can also open the resulting 'png' image  in Chrome url " +
                "https://cloudapps.herokuapp.com/imagetoascii/" +
                "\n||||||||||||||||||\n");

        final KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);

        // Always and unconditionally clean local state prior to starting the processing topology.
        // We opt for this unconditional call here because it will make it easier for us to play
        // around with the example when resetting the application for doing a re-run via the
        // Application Reset Tool
        // $CONFLUENT_HOME/bin/kafka-streams-application-reset
        // https://docs.confluent.io/current/streams/developer-guide/app-reset-tool.html#kstreams-application-reset-tool

        // The drawback of cleaning up local state prior to run is that your application must
        // rebuild it's local state from scratch, which will take time and will require reading
        // all the state relevant data from the Kafka cluster over the network

        // Thus in production you typically don't want to clean up always as we do here, but rather
        // when it is truly needed, i.e. under only certain conditions.
        // See `ApplicationResetExample.java` for a production-like example.
        streams.cleanUp();

        // Run the processor topology via start() to begin processing it's data
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close the Streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static void createStream(StreamsBuilder streamsBuilder) {
        // Key: word, value: count
        KStream<String, Long> wordCounts = streamsBuilder.stream(inputTopic,
                Consumed.with(Serdes.String(), Serdes.Long()));

        /**+++++++++++++++++++++++++++++++++++++++++++++++++++
         *++++++++++++++++++++++++++++++++++++++++++++++++++++
         *++++++++++++++++++++++++++++++++++++++++++++++++++++
         * windowedBy examples applied to KGROUPEDKSTREAM ++++
         *++++++++++++++++++++++++++++++++++++++++++++++++++++
         *++++++++++++++++++++++++++++++++++++++++++++++++++++
         ++++++++++++++++++++++++++++++++++++++++++++++++++++*/

        // Group the stream
        KGroupedStream<String, Long> groupedStream =
                wordCounts.groupByKey(Grouped.with(Serdes.String(), Serdes.Long()));

        /**********************************************
         * KGroupedStream 5 minutes tumbling windows:
         * Aggregate the groupedStream with time-based windowing, here
         * with 5 minutes tumbling windows
         **********************************************/
        KTable<Windowed<String>, Long> timeWindowedAggregatedStream =
            groupedStream
              .windowedBy(TimeWindows.of(Duration.ofMinutes(5)))
              .aggregate(
                () -> 0L,                                            // Initializer
                (aggKey, newValue, aggValue) -> aggValue + newValue, // Adder
                // This causes an error:
                // Materialized.as("aggregated-stream-store")           // State Store
                /*
                org.apache.kafka.streams.errors.StreamsException: ClassCastException
                while producing data to topic kstreams-from-topic-aggregated-table-store-repartition.
                A serializer (key: org.apache.kafka.common.serialization.StringSerializer / value:
                 org.apache.kafka.streams.kstream.internals.ChangedSerializer) is not compatible to
                 the actual key or value type (key type: java.lang.String / value type:
                 org.apache.kafka.streams.kstream.internals.Change). Change the default Serdes in
                 StreamConfig or provide correct Serdes via method parameters (for example if using the DSL,
                 `#to(String topic, Produced<K, V> produced)` with
                 `Produced.keySerde(WindowedSerdes.timeWindowedSerdeFrom(String.class))`).
                 */
                // This fixes the error
                Materialized
                .<String, Long, WindowStore<Bytes, byte[]>>as("time-5-min-tumbling-windows-aggregated-stream-store" /* state store name */)          // State Store name
                .withValueSerde(Serdes.Long())
        );

//        timeWindowedAggregatedStream.toStream().print(Printed.toSysOut());

        // Write the timeWindowedAggregatedStream to the output topic
        timeWindowedAggregatedStream.toStream().to(timeWindowedAggStreamOutputTopic);

        /**********************************************
         * KGroupedStream with an inactivity gap of 5 minutes:
         * Aggregate the groupedStream with session-based windowing
         * with an inactivity gap of 5 minutes
         **********************************************/
        KTable<Windowed<String>, Long> sessionizedAggregatedStream = groupedStream
                .windowedBy(SessionWindows.with(Duration.ofSeconds(5)))
                .aggregate(
                        () -> 0L,                                                              // Initializer
                        (aggKey, newValue, aggValue) -> newValue + aggValue,                   // Adder
                        (aggKey, leftAggValue, rightAggValue) -> leftAggValue + rightAggValue, // Session merger
                        // State Store name
                        Materialized.<String, Long, SessionStore<Bytes, byte[]>>as("session-agg-stream-store")
                                .withValueSerde(Serdes.Long())
                );

        sessionizedAggregatedStream.toStream().to(sessionizedAggStreamOutputTopic );

        /**++++++++++++++++++++++++++++++++++++++++++++++++++++
         *+++++++++++++++++++++++++++++++++++++++++++++++++++++
         *+++++++++++++++++++++++++++++++++++++++++++++++++++++
         * windowedBy examples applied to COGROUPEDKSTREAM ++++
         *+++++++++++++++++++++++++++++++++++++++++++++++++++++
         *+++++++++++++++++++++++++++++++++++++++++++++++++++++
         +++++++++++++++++++++++++++++++++++++++++++++++++++++*/

        // Create a cogroup from the grouped stream
        CogroupedKStream<String, Long> cogroupedStream =
                groupedStream.cogroup((aggKey, newValue, aggValue) ->
                        aggValue + newValue     // Adder
                );

        /**********************************************
         * CogroupedKStream 5 minutes tumbling windows:
         * Aggregating with time-based windowed with 5 min tumbling windows
         * Note in this example the required "adder" aggregator is specified in the
         * prior cogroup() call already
         **********************************************/
        KTable<Windowed<String>, Long> timeWindowedAggregatedStreamFromCogroup =
                cogroupedStream.windowedBy(TimeWindows.of(Duration.ofMinutes(5)))
                        .aggregate(
                                () -> 0L,   // Initializer
                                Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("time-windowed-aggregate-stream-store-from-cogroup")
                                        .withValueSerde(Serdes.Long())
                        );

        // Write the timeWindowedAggregatedStreamFromCogroup to the corresponding output topic
        timeWindowedAggregatedStreamFromCogroup.toStream()
                .to(timeWindowedAggStreamFromCogroupOutTopic,
                        Produced.keySerde(WindowedSerdes.timeWindowedSerdeFrom(String.class)));

        /**********************************************
         * CogroupedKStream with an inactivity gap of 5 minutes:
         * Aggregate the cogroupedStream with session-based windowing
         * with an inactivity gap of 5 minutes
         * Note in this example the required "adder" aggregator is specified in the
         * prior cogroup() call already
         **********************************************/
        KTable<Windowed<String>, Long> sessionizedAggregatedStreamFromCogroup =
                cogroupedStream
                        .windowedBy(SessionWindows.with(Duration.ofMinutes(5)))
                        .aggregate(
                            () -> 0L,                                                               // Initializer
                            (aggKey, leftAggValue, rightAggValue) -> leftAggValue + rightAggValue,  // Session merger
                            // State Store name
                            Materialized.<String, Long, SessionStore<Bytes, byte[]>>as("sessionized-aggregate-stream-store-from-cogroup")
                                    .withValueSerde(Serdes.Long())
                );
        // Write the sessionizedAggregatedStreamFromCogroup to the corresponding output topic
        sessionizedAggregatedStreamFromCogroup.toStream()
                .to(sessionizedAggStreamFromCogroupOutTopic,
                        Produced.keySerde(WindowedSerdes.sessionWindowedSerdeFrom(String.class)));
    }

    public static Properties getStreamsConfiguration(String bootstrapServers) {
        Properties properties = new Properties();

        // Give the streams an unique application name. The name must be unique
        // against which the application is running
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstreams-from-topic");
        properties.put(StreamsConfig.CLIENT_ID_CONFIG, "kstreams-from-topic-client");
        // Where to find the Kafka brokers
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Specify the default (de)serializers for keys and values
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                               Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                               Serdes.String().getClass().getName());

        // Records should be flushed every 10 seconds. This is less than the default in order
        // to keep this example interactive
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        // Disable record caches for ilustrative purposes
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        // Use a temporary directory for storing state. This will be automatically removed
        // after the test
        properties.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        // In case some message you have these options:
        // Option 1: Log the error and shut down the application (LogAndFailExceptionHandler)
        // Option 2: Skip corrupted records and log the error (LogAndContinueExceptionHandler)
        // Option 3: Quarantine corrupted records (dead letter queue)
        //    https://docs.confluent.io/current/streams/faq.html#option-3-quarantine-corrupted-records-dead-letter-queue
        properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                       LogAndContinueExceptionHandler.class);


        return properties;
    }
}
