package io.confluent.examples.streams.streamdsl.interactivequeries;

import io.confluent.common.utils.TestUtils;
import io.confluent.examples.streams.streamdsl.interactivequeries.processor.EmailMetricThresholdAlert_ProcessorSupplier;
import io.confluent.examples.streams.streamdsl.interactivequeries.statestore.MyCustomStoreBuilder;
import io.confluent.examples.streams.streamdsl.interactivequeries.statestore.MyCustomStoreType;
import io.confluent.examples.streams.streamdsl.interactivequeries.statestore.MyReadableCustomStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorSupplier;

import java.util.Properties;

/**
 * Demonstrates, using the high-level KStream DSL, how to query
 * Custom State Stores
 *
 **/
@Slf4j
public class O3_QueryCustomStores {
    static final String inputTopic = "input-topic";
    static final String stateStoreName = "my-custom-state-store";

    public static void main(String[] args, Topology streamBuilder) {

        final String bootstrapServers = args.length > 0? args[0]: "localhost:9092";

        // Configure the Streams application
        final Properties streamsConfiguration = getStreamsConfiguration(bootstrapServers);

        final Topology topology =  buildTheTopology();

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

        // Get access to "my-custom-state-store"
        MyReadableCustomStore<String, String> stateStore =
                streams.store(stateStoreName, new MyCustomStoreType<String, String>());
        // and then query it
        String value = stateStore.read("key");
        log.info("the value stored for key='key' is value={}", value);

        // Add shutdown hook to respond to SIGTERM and gracefully close the Streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static Topology buildTheTopology() {
        // Define the processing topology of the Streaming application
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        Topology topology = streamsBuilder.build();

        // Create a processor supplier
        ProcessorSupplier processorSupplier = new EmailMetricThresholdAlert_ProcessorSupplier();

        // TODO: WORK IN PROGRESS
        // Create a CustomStoreSupplier for store name "my-custom-state-store"
        MyCustomStoreBuilder customStoreBuilder = new MyCustomStoreBuilder(stateStoreName);

        // Add the source topic to the Topology
        topology.addSource("input-source", inputTopic);

        // Add a Custom Processor that reads from the Source Topic
        topology.addProcessor("my-custom-processor", processorSupplier, "input-source");

        // Connect the Custom State Store to the processor above
        topology.addStateStore(customStoreBuilder, "my-custom-processor");

        // No sink topic is added, as Processor doesn't produce records, just processes the input records
        // and generates third party actions

        return topology;
    }

    public static Properties getStreamsConfiguration(String bootstrapServers) {
        Properties properties = new Properties();

        // Give the streams an unique application name. The name must be unique
        // against which the application is running
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "query-custom-store");
        properties.put(StreamsConfig.CLIENT_ID_CONFIG, "query-custom-store-client");
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
