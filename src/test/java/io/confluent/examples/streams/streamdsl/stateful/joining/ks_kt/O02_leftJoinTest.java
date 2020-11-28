package io.confluent.examples.streams.streamdsl.stateful.joining.ks_kt;

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
 * Stream processing unit test of {@link O02_leftJoin}, using TopologyTestDriver.
 *
 * See {@link O02_leftJoin} for further documentation.
 */
public class O02_leftJoinTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> testLeftInputStreamTopic;
    private TestInputTopic<String, String> testRightInputTableTopic;
    private TestOutputTopic<String, String> testLeftJoinOutputTopic;
    private StringSerializer stringSerializer = new StringSerializer();
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

        testLeftInputStreamTopic =
                testDriver.createInputTopic(O02_leftJoin.leftInputStreamTopic,
                        stringSerializer, stringSerializer);
        testRightInputTableTopic =
                testDriver.createInputTopic(O02_leftJoin.rightInputTableTopic,
                        stringSerializer, stringSerializer);
        testLeftJoinOutputTopic =
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
        List<KeyValue<String, String>> leftInputValues =
                Arrays.asList(
                        new KeyValue<>("k", "A")
                );
        List<KeyValue<String, String>> rightInputValues =
                Arrays.asList(
                       new KeyValue<>("k", null) // Tombstone, but will generate output record
                );
        Map<String, String> outputValues = new HashMap<>();
        outputValues.put("k","left=A, right=null");

        // In order to trigger the join, first, the table must have matching elements
        testRightInputTableTopic.pipeKeyValueList(rightInputValues,
                Instant.ofEpochMilli(0), Duration.ofMillis(10000));
        // After the table has elements, records arriving to the stream will start triggering joins with
        // the matching elements in the table
        testLeftInputStreamTopic.pipeKeyValueList(leftInputValues,
                Instant.ofEpochMilli(0), Duration.ofMillis(10000));
        assertThat(testLeftJoinOutputTopic.readKeyValuesToMap(), equalTo(outputValues));
        //No more output in topic
        assertTrue(testLeftJoinOutputTopic.isEmpty());
    }

}
