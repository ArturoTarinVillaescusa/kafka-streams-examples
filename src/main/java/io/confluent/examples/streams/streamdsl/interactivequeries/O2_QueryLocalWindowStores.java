package io.confluent.examples.streams.streamdsl.interactivequeries;

import io.confluent.common.utils.TestUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Demonstrates, using the high-level KStream DSL, how to query
 * local KeyValue Stores
 *
 **/
@Slf4j
public class O2_QueryLocalWindowStores {
    static final String inputTopic = "input-topic";
    static final String outputTopic = "output-topic";
    static final String oddOutputTopic = "odd-output-topic";
    static final String pairOutputTopic = "pair-output-topic";
    static final String stateStoreName = "count-key-value-store";
    static final String oddStateStoreName = "odd-count-key-value-store";
    static final String pairStateStoreName = "pair-count-key-value-store";

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

        // Get access to “count-key-value-store” and then query it via the ReadOnlyKeyValueStore API:
        queryTheStateStore(streams, stateStoreName);

        // Add shutdown hook to respond to SIGTERM and gracefully close the Streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static void createStream(StreamsBuilder streamsBuilder) {
        KStream<String, String> textLinesStream = streamsBuilder.stream(inputTopic,
                Consumed.as("source-from-topic").with(Serdes.String(), Serdes.String()));

        // Define the processing topology, for example a typical word count
        KStream<String, String> wordsStream =
                textLinesStream.flatMapValues(
                        (value) -> Arrays.asList(value.toLowerCase().split("\\W+"))
                , Named.as("words-stream"));

        KGroupedStream<String, String> groupedByWord = wordsStream.groupBy((key, word) -> word,
                Grouped.as("kgrouped-words-stream")
                       .with(Serdes.String(), Serdes.String()));

        // Materialize the result of filtering corresponding to the count of different words by
        // creating a key-value State Store named "count-key-value-store" for the all-time words
        // count
        KTable<Windowed<String>, Long> countByGroup = groupedByWord
                .windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
                .count(Named.as("counting-words-occurrences"),
                        Materialized
                                .<String, Long, WindowStore<Bytes, byte[]>>as(stateStoreName)
                                .withValueSerde(Serdes.Long()));

        countByGroup.toStream().print(Printed.toFile("/tmp/countByGroup.txt"));


        // Write the pair wordCounts to the output topic
        countByGroup.toStream().to(outputTopic,
                Produced.as("sink-to-otuput-topic"));
    }

    private static void queryTheStateStore(KafkaStreams streams, String stateStoreName) {
        // Get the key-value State Store
        ReadOnlyKeyValueStore<Object, Object> keyValueStore =
                streams.store(StoreQueryParameters.fromNameAndType(stateStoreName,
                        QueryableStoreTypes.keyValueStore()));

        // Get the values for all of the keys available in this application instance
        keyValueStore.all().forEachRemaining(kv -> {
            System.out.println("key={},"+ kv.key+" value={}\n" +kv.value);
            log.info("key={}, value={}\n", kv.key, kv.value);
        });

    }

    public static Properties getStreamsConfiguration(String bootstrapServers) {
        Properties properties = new Properties();

        // Give the streams an unique application name. The name must be unique
        // against which the application is running
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "query-local-key-value-store");
        properties.put(StreamsConfig.CLIENT_ID_CONFIG, "query-local-key-value-store-client");
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
