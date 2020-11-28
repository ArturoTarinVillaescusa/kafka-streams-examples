package io.confluent.examples.streams.streamdsl.stateful.windowing;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Stream processing unit test of {@link O03_slidingWindow}, using TopologyTestDriver.
 *
 * See {@link O03_slidingWindow} for further documentation.
 */
public class O03_slidingWindowTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> testLeftInputStreamTopic;
    private TestInputTopic<String, String> testRightInputTableTopic;
    private TestOutputTopic<String, String> testInnerJoinOutputTopic;
    final StreamsBuilder builder = new StreamsBuilder();

    @Before
    public void setup() {

        // Create actual StreamBuilder topology
        O03_slidingWindow.createStream(builder);

        Topology topology = builder.build();

        System.out.println("\n||||||||||||||||||\n\n" + topology.describe() +
                "You can see it in http://zz85.github.io/kafka-streams-viz\n\n" +
                "Alternatively you can run ~/Downloads/apache-tomcat-9.0.39/bin/catalina.sh start\n" +
                "and use your local url http://localhost:8080/kafka-streams-viz/\n" +
                "If you want to play around, save the png graph topology obtained and open it in Chrome url " +
                "https://cloudapps.herokuapp.com/imagetoascii/" +
                "\n||||||||||||||||||\n");

        final TopologyTestDriver topologyTestDriver =
                new TopologyTestDriver(topology,
                        O03_slidingWindow
                                .getStreamsConfiguration("localhost:9092"));


        testLeftInputStreamTopic = topologyTestDriver
                .createInputTopic(O03_slidingWindow.leftInputStreamTopic,
                        new StringSerializer(),
                        new StringSerializer());
        testRightInputTableTopic = topologyTestDriver
                .createInputTopic(O03_slidingWindow.rightInputTableTopic,
                        new StringSerializer(),
                        new StringSerializer());
        testInnerJoinOutputTopic = topologyTestDriver
                .createOutputTopic(O03_slidingWindow.innerJoinOutputTopic,
                        new StringDeserializer(),
                        new StringDeserializer());

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
        List<KeyValue<String, String>> leftStreamInputValues =
                Arrays.asList(
                        new KeyValue<>("1", null)
                       ,new KeyValue<>("1", "A")
                );
        List<KeyValue<String, String>> rightTableInputValues =
                Arrays.asList(
                        new KeyValue<>("1", null)  // Tombstone
                        ,new KeyValue<>("1", "a")
                        ,new KeyValue<>("1", "b")
                        ,new KeyValue<>("1", null)  // Tombstone
                );
        Map<String, String> outputValues = new HashMap<>();
//        outputValues.put("1","Left=A, Right=b");

        // In order to trigger the join, first, the table must have matching elements
        testRightInputTableTopic.pipeKeyValueList(rightTableInputValues);
        // After the table has elements, records arriving to the stream will start triggering joins with
        // the matching elements in the table
        testLeftInputStreamTopic.pipeKeyValueList(leftStreamInputValues);

        assertThat(testInnerJoinOutputTopic.readKeyValuesToMap(), equalTo(outputValues));
        //No more output in topic
        assertTrue(testInnerJoinOutputTopic.isEmpty());
    }

}
