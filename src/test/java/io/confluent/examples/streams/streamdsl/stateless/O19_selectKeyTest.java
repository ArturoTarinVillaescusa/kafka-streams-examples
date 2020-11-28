package io.confluent.examples.streams.streamdsl.stateless;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Stream processing unit test of {@link O19_selectKey}, using TopologyTestDriver.
 *
 * See {@link O19_selectKey} for further documentation.
 */
public class O19_selectKeyTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<byte[], String> testInputTopic;
    private TestOutputTopic<String, String> testOutputTopic;
    private StringSerializer stringSerializer = new StringSerializer();
    private ByteArraySerializer byteArraySerializer = new ByteArraySerializer();
    private StringDeserializer stringDeserializer = new StringDeserializer();

    @Before
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();

        // Create actual StreamBuilder topology
        O19_selectKey.createStream(builder);

        Topology topology = builder.build();

        System.out.println("\n||||||||||||||||||\n\n" + topology.describe() +
                "You can see it in http://zz85.github.io/kafka-streams-viz\n\n" +
                "Alternatively you can run ~/Downloads/apache-tomcat-9.0.39/bin/catalina.sh start\n" +
                "and use your local url http://localhost:8080/kafka-streams-viz/\n" +
                "If you want to play around, save the png graph topology obtained and open it in Chrome url " +
                "https://cloudapps.herokuapp.com/imagetoascii/" +
                "\n||||||||||||||||||\n");

        testDriver = new TopologyTestDriver(topology,
                               O19_selectKey.getStreamsConfiguration("localhost:9092"));

        testInputTopic =
                testDriver.createInputTopic(O19_selectKey.inputTopic,
                        byteArraySerializer, stringSerializer);
        testOutputTopic =
                testDriver.createOutputTopic(O19_selectKey.outputTopic,
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
        List<KeyValue<byte[], String>> inputValues =
                Arrays.asList(
                        new KeyValue<>("Hello".getBytes(), "this sentence will be used to rekey"),
                        new KeyValue<>("How are".getBytes(), "sure? Yes, it will be for sure.")
                );

        Map<String, String> expectedOutput = new HashMap();
        expectedOutput.put("sure?", "sure? Yes, it will be for sure.");
        expectedOutput.put("this", "this sentence will be used to rekey");

        testInputTopic.pipeKeyValueList(inputValues,
                Instant.ofEpochMilli(0), Duration.ofMillis(100));

        assertThat(testOutputTopic.readKeyValuesToMap(), equalTo(expectedOutput));
        //No more output in topic
        assertTrue(testOutputTopic.isEmpty());
    }

}
