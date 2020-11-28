package io.confluent.examples.streams.streamdsl.stateless;

import io.confluent.common.utils.TestUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

/**
 * Demonstrates the use of the `groupByKey` high-level KStream DSL
 **/

public class O10_groupByKey {
    public static final String inputTopic = "streams-input";
    public static final String outputTopic = "streams-groupbykey-output";
    public static final String outputTopicDifferentValueType = "streams-groupbykeydifferentvaluetype-output";

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
        final KStream<String, Long> stream = streamsBuilder.stream(inputTopic,
                Consumed.with(Serdes.String(), Serdes.Long()));

        // Group by the existing key, using the application's configured
        // default SerDes for keys and values (see the getStreamsConfiguration(...)
        // function below
        final KGroupedStream<String, Long> groupedStream = stream.groupByKey();

        // Forcing a chane to the value type. This will have implications in groupByKey as we will see
        // in the next command
        final KStream<String, String> streamValuesTransformedToString = stream.mapValues(v -> String.valueOf(v));

        // When the key and/or value types do not match the configured
        // default serdes, we must explicitly specify serdes.
        // Here we have an example of this situation, where we have previously modified the
        // Group by the existing key, using a different SerDes for the value to the
        // value to a different type that the one configured by default in the application
        // (see the getStreamsConfiguration(...) function below)
        final KGroupedStream<String, String> groupedStreamValuesTransformedToString
                = streamValuesTransformedToString.groupByKey(Grouped.with(  Serdes.String(),
                                                                            Serdes.String()));
        // Write the groupByKey with original value types to the output topics
        final KTable<String, Long> countTuples = groupedStream.count();
        countTuples.toStream().to(outputTopic);

        // Write the groupByKey with transformed value types to the output topics
        final KTable<String, Long> countstreamValuesTransformedToStringTuples = groupedStreamValuesTransformedToString.count();

        countstreamValuesTransformedToStringTuples.toStream().print(Printed.toSysOut());
        countstreamValuesTransformedToStringTuples.toStream().print(Printed.toFile("/tmp/print-stream-example.txt"));
        countstreamValuesTransformedToStringTuples.toStream().foreach((k, v) -> System.out.println(k.getClass().getName()+"->"+v.getClass().getName()));

        groupedStreamValuesTransformedToString.count().toStream().to(outputTopicDifferentValueType);
    }

    public static Properties getStreamsConfiguration(final String bootstrapServers) {
        final Properties properties = new Properties();

        // Give the streams an unique application name. The name must be unique
        // against which the application is running
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstreams-groupbykey");
        properties.put(StreamsConfig.CLIENT_ID_CONFIG, "kstreams-groupbykey-client");
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
