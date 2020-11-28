package io.confluent.examples.streams.streamdsl.stateful.joining.kt_kt;

import org.apache.kafka.common.serialization.*;
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
 * Stream processing unit test of {@link O02_leftJoin}, using TopologyTestDriver.
 *
 * See {@link O02_leftJoin} for further documentation.
 */
public class O02_leftJoinTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Integer> testInputTopic1;
    private TestInputTopic<Integer, String> testInputTopic2;
    private TestOutputTopic<String, String> testInnerJoinOutputTopic;
    private StringSerializer stringSerializer = new StringSerializer();
    private IntegerSerializer intSerializer = new IntegerSerializer();
    private StringDeserializer stringDeserializer = new StringDeserializer();

    @Before
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();

        // Create actual StreamBuilder topology
        O02_leftJoin.createStream(builder);

        Topology topology = builder.build();

        System.out.println("\n||||||||||||||||||\n\n" + topology.describe() +
                "You can see it in http://zz85.github.io/kafka-streams-viz\n\n" +
                "Alternatively you can run ~/Downloads/apache-tomcat-9.0.39/bin/catalina.sh start\n" +
                "and use your local url http://localhost:8080/kafka-streams-viz/\n" +
                "If you want to play around, save the png graph topology obtained and open it in Chrome url " +
                "https://cloudapps.herokuapp.com/imagetoascii/" +
                "\n||||||||||||||||||\n");

        testDriver = new TopologyTestDriver(topology,
                               O02_leftJoin.getStreamsConfiguration("localhost:9092"));

        testInputTopic1 =
                testDriver.createInputTopic(O02_leftJoin.inputTopic1,
                        stringSerializer, intSerializer);
        testInputTopic2 =
                testDriver.createInputTopic(O02_leftJoin.inputTopic2,
                        intSerializer, stringSerializer);
        testInnerJoinOutputTopic =
                testDriver.createOutputTopic(O02_leftJoin.innerJoinOutputTopic,
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
                        ,new KeyValue<>("k",null) // Not a Tombstone because it doesn't remove
                                                  // the row with k key from the output KTable, it only
                                                  // assigns a null value to the "k" key in the output KT
                        ,new KeyValue<>("k",1)
                        ,new KeyValue<>("q",10)
                        ,new KeyValue<>("r",10)
                );
        List<KeyValue<Integer, String>> rightInputValues =
                Arrays.asList(
                        new KeyValue<>(1, "foo")
                        ,new KeyValue<>(3, "bar")
                        ,new KeyValue<>(10, "matches with both q and r keys")
                );
        Map<String, String> outputValues = new HashMap<>();
        outputValues.put("k","left=1, right=foo");
        outputValues.put("q","left=10, right=matches with both q and r keys");
        outputValues.put("r","left=10, right=matches with both q and r keys");


        testInputTopic1.pipeKeyValueList(leftInputValues,
                Instant.ofEpochMilli(0), Duration.ofMillis(100));
        testInputTopic2.pipeKeyValueList(rightInputValues,
                Instant.ofEpochMilli(0), Duration.ofMillis(100));
        assertThat(testInnerJoinOutputTopic.readKeyValuesToMap(), equalTo(outputValues));
        //No more output in topic
        assertTrue(testInnerJoinOutputTopic.isEmpty());
    }

}
