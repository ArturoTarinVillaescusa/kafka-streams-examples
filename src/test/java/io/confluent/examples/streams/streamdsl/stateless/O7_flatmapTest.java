package io.confluent.examples.streams.streamdsl.stateless;

import io.confluent.examples.streams.streamdsl.stateless.O7_flatMap;
import org.apache.kafka.common.serialization.IntegerDeserializer;
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
 * Stream processing unit test of {@link O7_flatMap}, using TopologyTestDriver.
 *
 * See {@link O7_flatMap} for further documentation.
 */
public class O7_flatmapTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<Long, String> testInputTopic;
    private TestOutputTopic<String, Integer> testOutputTopic;
    private StringSerializer stringSerializer = new StringSerializer();
    private LongSerializer longSerializer = new LongSerializer();
    private StringDeserializer stringDeserializer = new StringDeserializer();
    private IntegerDeserializer intDeserializer = new IntegerDeserializer();

    @Before
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();

        // Create actual StreamBuilder topology
        O7_flatMap.createStream(builder);

        Topology topology = builder.build();

        System.out.println("\n||||||||||||||||||\n\n" + topology.describe() +
                "You can see it in http://zz85.github.io/kafka-streams-viz\n\n" +
                "Alternatively you can run ~/Downloads/apache-tomcat-9.0.39/bin/catalina.sh start\n" +
                "and use your local url http://localhost:8080/kafka-streams-viz/\n" +
                "If you want to play around, save the png graph topology obtained and open it in Chrome url " +
                "https://cloudapps.herokuapp.com/imagetoascii/" +
                "\n||||||||||||||||||\n");

        testDriver = new TopologyTestDriver(topology,
                               O7_flatMap.getStreamsConfiguration("localhost:9092"));

        testInputTopic =
                testDriver.createInputTopic(O7_flatMap.inputTopic,
                                            longSerializer, stringSerializer);
        testOutputTopic =
                testDriver.createOutputTopic(O7_flatMap.outputTopic,
                                             stringDeserializer,intDeserializer);
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
        List<KeyValue<Long, String>> inputValues =
                Arrays.asList(new KeyValue<>(10L, "key01"), new KeyValue<>(33L, "key02"));
        Map<String, Integer> outputValues = new HashMap<>();
        outputValues.put("key01",0);
        outputValues.put("KEY01",10000);
        outputValues.put("key02",0);
        outputValues.put("KEY02",33000);

        testInputTopic.pipeKeyValueList(inputValues,
                Instant.ofEpochMilli(0), Duration.ofMillis(100));
        assertThat(testOutputTopic.readKeyValuesToMap(), equalTo(outputValues));
        //No more output in topic
        assertTrue(testOutputTopic.isEmpty());
    }

}
