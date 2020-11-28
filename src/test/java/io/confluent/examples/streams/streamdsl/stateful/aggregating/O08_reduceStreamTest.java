package io.confluent.examples.streams.streamdsl.stateful.aggregating;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Stream processing unit test of {@link O08_reduceStream}, using TopologyTestDriver.
 *
 * See {@link O08_reduceStream} for further documentation.
 */
public class O08_reduceStreamTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Long> testInputTopic;
    private TestOutputTopic<String, Long> testReduceStreamOutputTopic;
    private StringSerializer stringSerializer = new StringSerializer();
    private LongSerializer longSerializer = new LongSerializer();
    private StringDeserializer stringDeserializer = new StringDeserializer();
    private LongDeserializer longDeserializer = new LongDeserializer();

    @Before
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();

        // Create actual StreamBuilder topology
        O08_reduceStream.createStream(builder);

        Topology topology = builder.build();

        System.out.println("\n||||||||||||||||||\n\n" + topology.describe() +
                "You can see it in http://zz85.github.io/kafka-streams-viz\n\n" +
                "Alternatively you can run ~/Downloads/apache-tomcat-9.0.39/bin/catalina.sh start\n" +
                "and use your local url http://localhost:8080/kafka-streams-viz/\n" +
                "If you want to play around, save the png graph topology obtained and open it in Chrome url " +
                "https://cloudapps.herokuapp.com/imagetoascii/" +
                "\n||||||||||||||||||\n");

        testDriver = new TopologyTestDriver(topology,
                               O08_reduceStream.getStreamsConfiguration("localhost:9092"));

        testInputTopic =
                testDriver.createInputTopic(O08_reduceStream.inputTopic,
                                            stringSerializer, longSerializer);
        testReduceStreamOutputTopic =
                testDriver.createOutputTopic(O08_reduceStream.reducStreamOutputTopic,
                                             stringDeserializer,longDeserializer);
    }

    @After
    public void tearDown() {
        try {
            testDriver.close();
        } catch (final RuntimeException e) {
            // https://issues.apache.org/jira/browse/KAFKA-6647 causes exception when executed in Windows, ignoring it
            // Logged stacktrace cannot be avoided
            System.out.println("Ignoring exception, test failing in Windows due this exception:" + e.getLocalizedMessage());
        }
    }

    /**
     *  Simple test validating several input records
     */
    @Test
    public void outputTopicMustContainInputTopicRecords() {
        List<KeyValue<String, Long>> inputValues =
                Arrays.asList(
                        new KeyValue<>("Amancio",1L),
                        new KeyValue<>("Arturo",100L),
                        new KeyValue<>("Amancio",1L),
                        new KeyValue<>("Arturo",100L),
                        new KeyValue<>("Amancio",1L),
                        new KeyValue<>("Arturo",100L),
                        new KeyValue<>("Amancio",1L),
                        new KeyValue<>("Arturo",100L)
                );
        Map<String, Long> outputValues = new HashMap<>();
        outputValues.put("Arturo",400L);
        outputValues.put("Amancio",4L);

        testInputTopic.pipeKeyValueList(inputValues,
                Instant.ofEpochMilli(0), Duration.ofMillis(100));
        assertThat(testReduceStreamOutputTopic.readKeyValuesToMap(), equalTo(outputValues));
        //No more output in topic
        assertTrue(testReduceStreamOutputTopic.isEmpty());
    }

}
