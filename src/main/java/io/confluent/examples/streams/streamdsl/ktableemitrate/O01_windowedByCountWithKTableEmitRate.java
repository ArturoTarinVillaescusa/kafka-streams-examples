package io.confluent.examples.streams.streamdsl.ktableemitrate;

import io.confluent.common.utils.TestUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;

/**
 * Demonstrating the KStream DSL `windowdBy` and `count` methods applied together
 * controlling the KTable emit rate
 *
 * A KTable is logically a continuously updated table.
 *
 * These updates make their way to downsteam operators whenever new data is available,
 * ensuring that the whole computation is as fresh as possible.
 *
 * Most programs describe a series of logical transformations, and their update rate
 * is not a factor in the program behavior.
 *
 * In these cases, the rate of update is a performance concern, which best addressed
 * directly via the relevant configurations.
 *
 * However, for some applications, the rate of update itself is an important semantic property.
 *
 * Rather than achieving this as a side-effect of the record caches, we can directly impose
 * a rate limit via the KTable#supress operator.
 *
 * This configuration ensures that each key is updated no more than once every 5 minutes in stream time,
 * not wall-clock time.
 *
 * Note that the latest state for each key has to be buffered in memory for that 5 minute period.
 *
 * We have the option to control the maximum amount of memory to use for this buffer, in our example 1MB.
 * .maxBytes(1_000_000L)
 *
 * There's also an option to impose a limit in terms of number of records, or to leave both limits
 * unspecified too.
 * .maxRecords(10000L)
 *
 * Additionally it is possible to choose what happens if the buffer fills up.
 *
 * This example takes a relaxed approach and just emits the oldest records before their 5-minute time limit
 * to bring the buffer back down to size (.emitEarlyWhenFull()).
 *
 * Alternatively, we can choose to stop processing and shut the application down (.shutDownWhenFull()).
 *
 * This may seem extreme, but it gives you a guarantee that the 5-minutes time limit will be absolutely
 * enforced.
 *
 * After the application shuts down, you can allocate more memory for the buffer and resume processing.
 *
 * Emitting early is preferable for most applications than shutting down.
 **/

public class O01_windowedByCountWithKTableEmitRate {
    public static final String inputTopic = "input";
    public static final String timeWindowedCountOutTopic = "time-windowed-count-output";
    public static final String sessionWindowedCountOutTopic = "sessionized-count-output";

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

        // Group the stream
        KGroupedStream<String, Long> groupedStream =
                wordCounts.groupByKey(Grouped.with(Serdes.String(), Serdes.Long()));

        /**********************************************
         * Counting a KGroupedStream with time-based windowing
         * with 5 minutes tumbling windows
         **********************************************/
        KTable<Windowed<String>, Long> timeWindowedCountStream =
            groupedStream
              .windowedBy(TimeWindows.of(Duration.ofMinutes(5)))
              .count()
              // each key is updated no more than once every 5 minutes in stream time,
              // not wall-clock time.
              .suppress(Suppressed.untilTimeLimit(Duration.ofMinutes(5),
                                                  Suppressed.BufferConfig
                                                          // maximum amount of memory to use
                                                          // for this buffer
                                                          .maxBytes(1_000_000L)
                                                          // maximum number of records to use
                                                          // for this buffer
                                                          .withMaxRecords(10000L)
                                                          // emit the oldest records before their
                                                          // 5-minute time limit to bring the
                                                          //buffer back down to size
                                                          .emitEarlyWhenFull()
                                                )
                        );

//        timeWindowedCountStream.toStream().print(Printed.toSysOut());

        // Write the timeWindowedAggregatedStream to the output topic
        timeWindowedCountStream.toStream().to(timeWindowedCountOutTopic);

        /**********************************************
         * Counting a KGroupedStream with session-based windowing
         * with 5 minutes inactivity gaps
         **********************************************/
        KTable<Windowed<String>, Long> sessionWindowedCountStream =
                groupedStream
                        .windowedBy(SessionWindows.with(Duration.ofMinutes(5)))
                        .count();

//        sessionWindowedCountStream.toStream().print(Printed.toSysOut());

        // Write the timeWindowedAggregatedStream to the output topic
        sessionWindowedCountStream.toStream().to(sessionWindowedCountOutTopic);

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
