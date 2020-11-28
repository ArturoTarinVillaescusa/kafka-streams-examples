package io.confluent.examples.streams.streamdsl.stateless;

import io.confluent.common.utils.TestUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * Demonstrates the use of the `flatmap` high-level KStream DSL
 * Marks the stream for data re-partitioning: Applying a grouping or a join after flatMap
 * will result in re-partitioning of the records.
 * If possible use flatMapValues instead, which will not cause data re-partitioning.
 **/

public class O7_flatMap {
    public static final String inputTopic = "streams-input";
    public static final String outputTopic = "streams-flatmap-output";

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
        final KStream<Long, String> stream = streamsBuilder.stream(inputTopic,
                Consumed.with(Serdes.Long(), Serdes.String()));

        // apply a flatMap transformation to the stream topology.
        // Marks the stream for data re-partitioning: Applying a grouping or
        // a join after flatMap will result in re-partitioning of the records.
        // If possible use flatMapValues instead, which will not cause data re-partitioning.
        final KStream<String, Integer> flatmappedValues = stream.flatMap(
                // Here, we generate two output records for each input record.
                // We also change the key and value types.
                // Example: (1000L, "Hello") -> ("HELLO", 1000000), ("hello", 1)
                (key, value) -> {
                    final List<KeyValue<String, Integer>> result = new LinkedList<>();
                    result.add(KeyValue.pair(value.toUpperCase(), Integer.valueOf((int) (key * 1000))));
                    result.add(KeyValue.pair(value.toLowerCase(), Integer.valueOf((int) (key / 1000))));
                    return result;
                }
        );

        // Write the flatMapped transformed values to the output topic
        flatmappedValues.to(outputTopic, Produced.with(Serdes.String(), Serdes.Integer()));
    }

    public static Properties getStreamsConfiguration(final String bootstrapServers) {
        final Properties properties = new Properties();

        // Give the streams an unique application name. The name must be unique
        // against which the application is running
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstreams-flatmap");
        properties.put(StreamsConfig.CLIENT_ID_CONFIG, "kstreams-flatmap-client");
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
