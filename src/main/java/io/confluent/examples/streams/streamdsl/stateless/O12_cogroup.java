package io.confluent.examples.streams.streamdsl.stateless;

import io.confluent.common.utils.TestUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

/**
 * Demonstrates the use of the `cogroup` high-level KStream DSL
 **/

public class O12_cogroup {
    public static final String inputOneTopic = "streams-input-one";
    public static final String inputTwoTopic = "streams-input-two";
    public static final String aggregatedStreamStore = "aggregated-stream-store";
    public static final String outputTopic = "aggregated-cogroup-output";

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
        // Construct a KStream from the inputOneTopic
        KStream<byte[], String> streamOne = streamsBuilder.stream(inputOneTopic,
                Consumed.with(Serdes.ByteArray(), Serdes.String()));

        // Group the stream by a new key and key type
        KGroupedStream<Object, String> groupedStreamOne = streamOne.groupBy((key, value) -> value);

        // Construct a KStream from the inputTwoTopic
        KStream<byte[], Long> streamTwo = streamsBuilder.stream(inputTwoTopic,
                Consumed.with(Serdes.ByteArray(), Serdes.Long()));

        // Group the stream by a new key and key type
        KGroupedStream<Object, Long> groupedStreamTwo = streamTwo.groupBy((key, value) -> value);

        // Create new cogroup from the first stream. The value type of the CogroupedStream
        // is the value type of the final aggregation result
        CogroupedKStream<Object, Integer> cogroupedStream =
                groupedStreamOne.cogroup((aggKey, newValue, aggValue) ->
                        aggValue + Integer.parseInt(newValue)     // Adder for first stream
                );

        // Add the second stream to the existing cogroup.
        // Note that the second input stream has a different type value than the first input stream.
        CogroupedKStream<Object, Integer> cogroup = cogroupedStream.cogroup(groupedStreamTwo,
                (aggKey, newValue, aggValue) -> aggValue + newValue.intValue() // Adder for second stream
        );

        // Aggregate all the streams of the cogroup
        final KTable<Object, Integer> aggregatedTable = cogroup.aggregate(
                () -> 0, // Initializer
                Materialized.as(aggregatedStreamStore) // State store name
        );

        aggregatedTable.toStream().to(outputTopic);
    }

    public static Properties getStreamsConfiguration(String bootstrapServers) {
        Properties properties = new Properties();

        // Give the streams an unique application name. The name must be unique
        // against which the application is running
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstreams-groupby");
        properties.put(StreamsConfig.CLIENT_ID_CONFIG, "kstreams-groupby-client");
        // Where to find the Kafka brokers
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Specify the default (de)serializers for keys and values
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                               Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                               Serdes.Long().getClass().getName());

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
