package io.confluent.examples.streams.streamdsl.stateful.aggregating;

import io.confluent.common.utils.TestUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

/**
 * Demonstrating the KStream DSL `aggregate` method applied to a KStream
 **/

public class O01_aggregate {
    static final String inputTopic = "input";
    static final String aggStreamOutputTopic = "aggregated-stream-output";
    static final String aggTableOutputTopic = "aggregated-table-output";

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
        // Construct a KStream from the inputTopic where message values represent
        // lines of text.
        KStream<byte[], String> stream = streamsBuilder.stream(inputTopic,
                Consumed.with(Serdes.ByteArray(), Serdes.String()));
        // Group the stream
        KGroupedStream<byte[], String> groupedStream =
                stream.groupByKey(Grouped.with(Serdes.ByteArray(), Serdes.String()));
        // Aggregate the groupedStream
        KTable<byte[], Long> aggregatedStream = groupedStream.aggregate(
                () -> 0L,                                                     // Initializer
                (aggKey, newValue, aggValue) -> aggValue + newValue.length(), // Adder
                Materialized.as("aggregated-stream-store")                    // State Store name
        );

        aggregatedStream.toStream().print(Printed.toSysOut());

        // Construct a KTable from the KStream
        KTable<byte[], String> table = stream.toTable();
        // Group the table
        KGroupedTable<String, String> groupedTable = table.groupBy((k, v) -> KeyValue.pair(v, v));
        // Aggregate the KTable
        KTable<String, Long> aggregatedKTable = groupedTable.aggregate(
                () -> 0L,                                                       // Initializer
                (aggKey, newValue, aggValue) -> aggValue + newValue.length(),   // Adder
                (aggKey, oldValue, aggValue) -> aggValue - oldValue.length(),   // Subtractor
                Materialized.as("aggregated-table-store")                       // State Store name
        );

        // Write the aggregatedStream to the output topic
        aggregatedStream.toStream().to(aggStreamOutputTopic);
        // Write the aggregatedTable to the output topic
        aggregatedKTable.toStream().to(aggTableOutputTopic);

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
