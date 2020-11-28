package io.confluent.examples.streams.streamdsl.stateful.joining.kt_kt;

import io.confluent.common.utils.TestUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Properties;
import java.util.function.Function;

/**
 * Demonstrating the KStream DSL `leftJoin` method applied to a KTable-KTable pair
 **/

public class O02_leftJoin {
    static final String inputTopic1 = "input-ktable-1";
    static final String inputTopic2 = "input-ktable-2";
    static final String innerJoinOutputTopic = "leftjoin-ktable-output";

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

    public static void createStream(StreamsBuilder builder) {
        // KTable from inputTopic1
        KTable<String, Integer> left = builder.table(inputTopic1,
              Consumed.with(Serdes.String(),
                            Serdes.Integer())
         );
        // KTable from inputTopic2
        KTable<Integer, String> right = builder.table(inputTopic2,
                Consumed.with(Serdes.Integer(),
                        Serdes.String())
        );

        // This foreignKeyExtractor simply uses the left-value to map to the right key
        Function<Integer, Integer> foreignKeyExtracctor = (x) -> x;

        KTable<String, String> joined = left.leftJoin(
                right,                                          // Right stream
                foreignKeyExtracctor,
                (leftValue, rightValue) ->                      // ValueJoiner
                        "left=" + leftValue + ", right=" + rightValue
        );

        left.toStream().print(Printed.toFile("/tmp/left.txt"));
        right.toStream().print(Printed.toFile("/tmp/right.txt"));
        joined.toStream().print(Printed.toFile("/tmp/joined.txt"));

        // Write the result to the output topic
        joined.toStream().to(innerJoinOutputTopic);

    }

    public static Properties getStreamsConfiguration(String bootstrapServers) {
        Properties properties = new Properties();

        // Give the streams an unique application name. The name must be unique
        // against which the application is running
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktables-leftjoin");
        properties.put(StreamsConfig.CLIENT_ID_CONFIG, "ktables-leftjoin-client");
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
