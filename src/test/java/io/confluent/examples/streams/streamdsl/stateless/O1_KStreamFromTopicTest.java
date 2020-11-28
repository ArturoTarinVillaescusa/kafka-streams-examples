package io.confluent.examples.streams.streamdsl.stateless;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
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
 * Stream processing unit test of {@link O1_KStreamFromTopic}, using TopologyTestDriver.
 *
 * See {@link O1_KStreamFromTopic} for further documentation.
 */
public class O1_KStreamFromTopicTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> testInputTopic;
    private TestOutputTopic<String, String> testOutputTopic;
    private StringSerializer stringSerializer = new StringSerializer();
    private StringDeserializer stringDeserializer = new StringDeserializer();

    @Before
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();

        // Create actual StreamBuilder topology
        O1_KStreamFromTopic.createStream(builder);

        Topology topology = builder.build();

        System.out.println("\n||||||||||||||||||\n\n" + topology.describe() +
                "You can see it in http://zz85.github.io/kafka-streams-viz\n\n" +
                "Alternatively you can run ~/Downloads/apache-tomcat-9.0.39/bin/catalina.sh start\n" +
                "and use your local url http://localhost:8080/kafka-streams-viz/\n" +
                "If you want to play around, save the png graph topology obtained and open it in Chrome url " +
                "https://cloudapps.herokuapp.com/imagetoascii/" +
                "\n||||||||||||||||||\n");

        testDriver = new TopologyTestDriver(topology,
                               O1_KStreamFromTopic.getStreamsConfiguration("localhost:9092"));

        testInputTopic =
                testDriver.createInputTopic(O1_KStreamFromTopic.inputTopic,
                                            stringSerializer, stringSerializer);
        testOutputTopic =
                testDriver.createOutputTopic(O1_KStreamFromTopic.outputTopic,
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
     *  Simple test validating of one input record
     */
    @Test
    public void testOneRecord() {
        // Feed one record to the testInputTopic. Timestamp is irrelevant in this occasion
        TestRecord<String, String> record = new TestRecord("key01","value01");
        testInputTopic.pipeInput(record);

        // Read and validate the output
        KeyValue<String, String> output = testOutputTopic.readKeyValue();
        assertThat(output, equalTo(KeyValue.pair("key01","value01")));
        //No more output in topic
        assertTrue(testOutputTopic.isEmpty());

    }

    /**
     *  Simple test validating several input records
     */
    @Test
    public void outputTopicMustContainInputTopicRecords() {
        List<KeyValue<String, String>> inputValues =
                Arrays.asList(new KeyValue<>("key01","value01"), new KeyValue<>("key02","value02"));
        Map<String, String> outputValues = new HashMap<>();
        outputValues.put("key01","value01");
        outputValues.put("key02","value02");

        testInputTopic.pipeKeyValueList(inputValues,
                Instant.ofEpochMilli(0), Duration.ofMillis(100));
        assertThat(testOutputTopic.readKeyValuesToMap(), equalTo(outputValues));
        //No more output in topic
        assertTrue(testOutputTopic.isEmpty());
    }

}
