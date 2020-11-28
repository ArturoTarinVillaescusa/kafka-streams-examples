package io.confluent.examples.streams.streamdsl.processorapi.transform;

import io.confluent.common.utils.TestUtils;
import io.confluent.examples.streams.streamdsl.processorapi.utils.transformers.EmailMetricThresholdAlert_Transformer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Properties;

/**
 * DSL Transformer API integration using State Store:
 * ===============================================
 *
 * In order for the transformer to use the State Stores, the stores must be added to the topology and
 * connected to the transformer using at least one of two strategies, though it's not required to connect
 * global state stores; read-only access to global state stores is available by default:
 *
 * 1) The first strategy (DEMOSTRATED IN THIS EXAMPLE) is to manually add the StoreBuilders via
 *    Topology.addStateStore(StoreBuilder, String, ...) and specify the store names via stateStoreNames
 *    so they will be connected to the transformer.
 *
 *
 * 2) The second strategy is for the given TransformerSupplier to implement ConnectedStoreProvider.stores(),
 *    which provides the StoreBuilders to be automatically added to the topology and connected to the
 *    transformer.
 **/

@Slf4j
public class O01_transformStrategy1_ImplementTransformer {
    static final String inputTopic = "input-topic";
    static final String outputTopic = "transformed-output";

    // A mocked schema registry for our serdes to use
    static final String MOCK_SCHEMA_REGISTRY_URL = "mock://localhost:8081";

    static final ZoneId zone = ZoneOffset.UTC;
    public static String stateStoreName = "myTransformerStateStore";

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

        // Key (String) is user ID, value is the page view event for that user.
        // Such a data stream is often called a "clickstream".
        KStream<String, String> sourceStream =
                streamsBuilder.stream(inputTopic, Consumed.as("SOURCE-STREAM"));

        sourceStream.print(Printed.toFile("/tmp/pageViews.txt"));

        // https://stackoverflow.com/questions/40221539/how-to-add-a-custom-statestore-to-the-kafka-streams-dsl-processorhttps://stackoverflow.com/questions/40221539/how-to-add-a-custom-statestore-to-the-kafka-streams-dsl-processor

        // Create the State Store
        StoreBuilder<KeyValueStore<String, String>> keyValueStoreBuilder =
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStoreName),
                                            Serdes.String(),
                                            Serdes.String());

        // Add (register) the State Store to the builder
        streamsBuilder.addStateStore(keyValueStoreBuilder);

        KStream transformedStream = sourceStream.transform(new TransformerSupplier() {
            @Override
            public Transformer get() {
                return new EmailMetricThresholdAlert_Transformer();
            }
        }, stateStoreName);

        transformedStream.print(Printed.toFile("/tmp/pageViewsAfterApplyingTheFilterTransformer.txt"));

        transformedStream.to(outputTopic,
                Produced.with(Serdes.Integer(), Serdes.String()));
    }

    public static Properties getStreamsConfiguration(String bootstrapServers) {
        Properties properties = new Properties();

        // Give the streams an unique application name. The name must be unique
        // against which the application is running
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "tumbling-window");
        properties.put(StreamsConfig.CLIENT_ID_CONFIG, "tumbling-window-client");
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

        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);

        return properties;
    }


}
