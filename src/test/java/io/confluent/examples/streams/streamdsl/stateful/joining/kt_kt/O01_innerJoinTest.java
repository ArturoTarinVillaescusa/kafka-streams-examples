package io.confluent.examples.streams.streamdsl.stateful.joining.kt_kt;

import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
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
 * Stream processing unit test of {@link O01_innerJoin}, using TopologyTestDriver.
 *
 * See {@link O01_innerJoin} for further documentation.
 */
public class O01_innerJoinTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Integer> testInputTopic1;
    private TestInputTopic<Integer, String> testInputTopic2;
    private TestOutputTopic<String, String> testInnerJoinOutputTopic;
    private StringSerializer stringSerializer = new StringSerializer();
    private IntegerSerializer intSerializer = new IntegerSerializer();
    private DoubleSerializer doubleSerializer = new DoubleSerializer();
    private StringDeserializer stringDeserializer = new StringDeserializer();

    @Before
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();

        // Create actual StreamBuilder topology
        O01_innerJoin.createStream(builder);

        Topology topology = builder.build();

        System.out.println("\n||||||||||||||||||\n\n" + topology.describe() +
                "You can see it in http://zz85.github.io/kafka-streams-viz\n\n" +
                "Alternatively you can run ~/Downloads/apache-tomcat-9.0.39/bin/catalina.sh start\n" +
                "and use your local url http://localhost:8080/kafka-streams-viz/\n" +
                "If you want to play around, save the png graph topology obtained and open it in Chrome url " +
                "https://cloudapps.herokuapp.com/imagetoascii/" +
                "\n||||||||||||||||||\n");

        testDriver = new TopologyTestDriver(topology,
                               O01_innerJoin.getStreamsConfiguration("localhost:9092"));

        testInputTopic1 =
                testDriver.createInputTopic(O01_innerJoin.inputTopic1,
                        stringSerializer, intSerializer);
        testInputTopic2 =
                testDriver.createInputTopic(O01_innerJoin.inputTopic2,
                        intSerializer, stringSerializer);
        testInnerJoinOutputTopic =
                testDriver.createOutputTopic(O01_innerJoin.innerJoinOutputTopic,
                                             stringDeserializer,stringDeserializer);
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
        List<KeyValue<String, Integer>> leftInputValues =
                Arrays.asList(
                         new KeyValue<>("k",1)
                        ,new KeyValue<>("k",2)
                        ,new KeyValue<>("k",3)
                        ,new KeyValue<>("k",null) // It's not a Tombstone, just assigns null value to the "k" key
                        ,new KeyValue<>("k",1)
                        ,new KeyValue<>("q",10)
                        ,new KeyValue<>("r",10)
                );
        List<KeyValue<Integer, String>> rightInputValues =
                Arrays.asList(
                         new KeyValue<>(1, "foo")
                        ,new KeyValue<>(2, "new value")
                        ,new KeyValue<>(3, "bar")
                        ,new KeyValue<>(10, "bar from q key")
                        ,new KeyValue<>(10, "bar from r key")
                );
        Map<String, String> outputValues = new HashMap<>();
        outputValues.put("k","left=1, right=foo");
        outputValues.put("q","left=10, right=bar from r key");
        outputValues.put("r","left=10, right=bar from r key");

        testInputTopic1.pipeKeyValueList(leftInputValues,
                Instant.ofEpochMilli(0), Duration.ofMillis(100));
        testInputTopic2.pipeKeyValueList(rightInputValues,
                Instant.ofEpochMilli(0), Duration.ofMillis(100));
        assertThat(testInnerJoinOutputTopic.readKeyValuesToMap(), equalTo(outputValues));
        //No more output in topic
        assertTrue(testInnerJoinOutputTopic.isEmpty());
    }

}
