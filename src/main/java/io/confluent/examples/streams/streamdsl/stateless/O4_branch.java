package io.confluent.examples.streams.streamdsl.stateless;

import io.confluent.common.utils.TestUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

/**
 * Demonstrates, using the high-level KStream DSL, how to split
 * in several branches or substreams a source KStream
 **/

public class O4_branch {
    static final String inputTopic = "streams-plaintext-input";
    static final String outputTopic0to9 = "streams-output-0-to-9";
    static final String outputTopic10to99 = "streams-output-10-to-99";
    static final String outputTopicRestOfTheKeys = "streams-output-rest-of-keys";

    public static void main(final String[] args) {

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

    public static void createStream(final StreamsBuilder streamsBuilder) {
        // Construct a KStream from the inputTopic where message values represent
        // lines of text.
        // In this example we ignore whatever may be stored in the keys.
        final KStream<String, String> stream = streamsBuilder.stream(inputTopic,
                Consumed.with(Serdes.String(), Serdes.String()));

        // In this example we are applying branch, which is a stateless topology
        // transformation in order to split in several branches or substreams a
        // source KStream
        final KStream<String, String>[] branches = stream.branch(
                (key, value) -> key.length() == 1,  // Keys between 0 and 9
                (key, value) -> key.length() == 2,  // Keys between 10 and 99
                (key, value) -> true                // Rest of the keys
                );

        final KStream<String, String> keysBetween0and9 = branches[0];
        final KStream<String, String> keysBetween10and99 = branches[1];
        final KStream<String, String> restOfTheKeys = branches[2];

        // Write the branches to their corresponding topics
        keysBetween0and9.to(outputTopic0to9, Produced.with(Serdes.String(), Serdes.String()));
        keysBetween10and99.to(outputTopic10to99, Produced.with(Serdes.String(), Serdes.String()));
        restOfTheKeys.to(outputTopicRestOfTheKeys, Produced.with(Serdes.String(), Serdes.String()));
    }

    public static Properties getStreamsConfiguration(final String bootstrapServers) {
        final Properties properties = new Properties();

        // Give the streams an unique application name. The name must be unique
        // against which the application is running
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "branch-example");
        properties.put(StreamsConfig.CLIENT_ID_CONFIG, "branch-example-client");
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
