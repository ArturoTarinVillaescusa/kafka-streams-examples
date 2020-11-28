package io.confluent.examples.streams.streamdsl.stateless;

import io.confluent.examples.streams.streamdsl.stateless.O09_foreach;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Stream processing unit test of {@link O09_foreach}, using TopologyTestDriver.
 *
 * See {@link O09_foreach} for further documentation.
 */
public class O09_foreachTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<byte[], String> testInputTopic;
    private TestOutputTopic<byte[], String> testOutputTopic;
    private StringSerializer stringSerializer = new StringSerializer();
    private ByteArraySerializer byteArraySerializer = new ByteArraySerializer();
    private StringDeserializer stringDeserializer = new StringDeserializer();
    private ByteArrayDeserializer byteArrayDeserializer = new ByteArrayDeserializer();;


    @Before
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();

        // Create actual StreamBuilder topology
        O09_foreach.createStream(builder);

        Topology topology = builder.build();

        System.out.println("\n||||||||||||||||||\n\n" + topology.describe() +
                "You can see it in http://zz85.github.io/kafka-streams-viz\n\n" +
                "Alternatively you can run ~/Downloads/apache-tomcat-9.0.39/bin/catalina.sh start\n" +
                "and use your local url http://localhost:8080/kafka-streams-viz/\n" +
                "If you want to play around, save the png graph topology obtained and open it in Chrome url " +
                "https://cloudapps.herokuapp.com/imagetoascii/" +
                "\n||||||||||||||||||\n");

        testDriver = new TopologyTestDriver(topology,
                               O09_foreach.getStreamsConfiguration("localhost:9092"));

        testInputTopic =
                testDriver.createInputTopic(O09_foreach.inputTopic,
                        byteArraySerializer, stringSerializer);
        testOutputTopic =
                testDriver.createOutputTopic(O09_foreach.outputTopic,
                        byteArrayDeserializer, stringDeserializer);
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
        List<KeyValue<byte[], String>> inputValues =
                Arrays.asList(  new KeyValue<>("1".getBytes(), "Hello World!"),
                                new KeyValue<>("33".getBytes(), "How are you today?"));
        List<String> outputValues = new ArrayList<>();
        outputValues.add("Hello");
        outputValues.add("World!");
        outputValues.add("How");
        outputValues.add("are");
        outputValues.add("you");
        outputValues.add("today?");

        testInputTopic.pipeKeyValueList(inputValues,
                Instant.ofEpochMilli(0), Duration.ofMillis(100));
        assertThat(testOutputTopic.readValuesToList(), equalTo(outputValues));
        //No more output in topic
        assertTrue(testOutputTopic.isEmpty());
    }

}
