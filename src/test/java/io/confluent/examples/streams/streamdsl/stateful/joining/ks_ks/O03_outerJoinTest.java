package io.confluent.examples.streams.streamdsl.stateful.joining.ks_ks;

import org.apache.kafka.common.serialization.DoubleSerializer;
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
 * Stream processing unit test of {@link O03_outerJoin}, using TopologyTestDriver.
 *
 * See {@link O03_outerJoin} for further documentation.
 */
public class O03_outerJoinTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Long> testInputTopic1;
    private TestInputTopic<String, Double> testInputTopic2;
    private TestOutputTopic<String, String> testOuterJoinOutputTopic;
    private StringSerializer stringSerializer = new StringSerializer();
    private DoubleSerializer doubleSerializer = new DoubleSerializer();
    private StringDeserializer stringDeserializer = new StringDeserializer();
    private LongSerializer longSerializer = new LongSerializer();

    @Before
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();

        // Create actual StreamBuilder topology
        O03_outerJoin.createStream(builder);

        Topology topology = builder.build();

        System.out.println("\n||||||||||||||||||\n\n" + topology.describe() +
                "You can see it in http://zz85.github.io/kafka-streams-viz\n\n" +
                "Alternatively you can run ~/Downloads/apache-tomcat-9.0.39/bin/catalina.sh start\n" +
                "and use your local url http://localhost:8080/kafka-streams-viz/\n" +
                "Use this Chrome url if you want the png png graph topology in text:" +
                "https://cloudapps.herokuapp.com/imagetoascii/" +
                "\n||||||||||||||||||\n");

        testDriver = new TopologyTestDriver(topology,
                               O03_outerJoin.getStreamsConfiguration("localhost:9092"));

        testInputTopic1 =
                testDriver.createInputTopic(O03_outerJoin.inputTopic1,
                        stringSerializer, longSerializer);
        testInputTopic2 =
                testDriver.createInputTopic(O03_outerJoin.inputTopic2,
                        stringSerializer, doubleSerializer);
        testOuterJoinOutputTopic =
                testDriver.createOutputTopic(O03_outerJoin.outerJoinOutputTopic,
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
        List<KeyValue<String, Long>> leftInputValues =
                Arrays.asList(
                        new KeyValue<>("key01",1L),
                        new KeyValue<>("key02",2L),
                        new KeyValue<>("k3",10L),
                        new KeyValue<>("k3",20L),
                        new KeyValue<>("k3",null),
                        new KeyValue<>("k3",30L),
                        new KeyValue<>("k3",null),
                        new KeyValue<>("k3",40L)
                );
        List<KeyValue<String, Double>> rightInputValues =
                Arrays.asList(
                        new KeyValue<>("key01",1.0),
                        new KeyValue<>("key02",2.0),
                        new KeyValue<>("k3", 1.0),
                        new KeyValue<>("k3", 2.0),
                        new KeyValue<>("k3", null),
                        new KeyValue<>("k3", 3.0),
                        new KeyValue<>("k3", null),
                        new KeyValue<>("k3", null),
                        new KeyValue<>("k3", 4.0)
                );
        Map<String, String> outputValues = new HashMap<>();
        outputValues.put("key01","left=1, right=1.0");
        outputValues.put("key02","left=2, right=2.0");
        outputValues.put("k3","left=10, right=2.0");
        outputValues.put("k3","left=20, right=2.0");
        outputValues.put("k3","left=30, right=1.0");
        outputValues.put("k3","left=30, right=2.0");
        outputValues.put("k3","left=30, right=3.0");
        outputValues.put("k3","left=30, right=4.0");
        outputValues.put("k3","left=40, right=1.0");
        outputValues.put("k3","left=40, right=2.0");
        outputValues.put("k3","left=40, right=3.0");
        outputValues.put("k3","left=40, right=4.0");

        testInputTopic1.pipeKeyValueList(leftInputValues,
                Instant.ofEpochMilli(0), Duration.ofMillis(100));
        testInputTopic2.pipeKeyValueList(rightInputValues,
                Instant.ofEpochMilli(0), Duration.ofMillis(100));
        assertThat(testOuterJoinOutputTopic.readKeyValuesToMap(), equalTo(outputValues));
        //No more output in topic
        assertTrue(testOuterJoinOutputTopic.isEmpty());
    }

}
