package io.confluent.examples.streams.streamdsl.stateless;

import io.confluent.examples.streams.streamdsl.stateless.O11_groupBy;
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
 * Stream processing unit test of {@link O11_groupBy}, using TopologyTestDriver.
 *
 * See {@link O11_groupBy} for further documentation.
 */
public class O11_groupByTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<byte[], String> testInputTopic;
    private TestOutputTopic<String, Long> testOutputGeneratedGroupsFromStreamTopic;
    private TestOutputTopic<String, Long> testOutputGeneratedGroupsFromTableTopic;
    private StringSerializer stringSerializer = new StringSerializer();
    private LongSerializer integerSerializer = new LongSerializer();
    private ByteArraySerializer byteArraySerializer = new ByteArraySerializer();
    private StringDeserializer stringDeserializer = new StringDeserializer();
    private LongDeserializer longDeserializer = new LongDeserializer();;


    @Before
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();

        // Create actual StreamBuilder topology
        O11_groupBy.createStream(builder);

        Topology topology = builder.build();

        System.out.println("\n||||||||||||||||||\n\n" + topology.describe() +
                "You can see it in http://zz85.github.io/kafka-streams-viz\n\n" +
                "Alternatively you can run ~/Downloads/apache-tomcat-9.0.39/bin/catalina.sh start\n" +
                "and use your local url http://localhost:8080/kafka-streams-viz/\n" +
                "If you want to play around, save the png graph topology obtained and open it in Chrome url " +
                "https://cloudapps.herokuapp.com/imagetoascii/" +
                "\n||||||||||||||||||\n");

        testDriver = new TopologyTestDriver(topology,
                               O11_groupBy.getStreamsConfiguration("localhost:9092"));

        testInputTopic =
                testDriver.createInputTopic(O11_groupBy.inputTopic,
                        byteArraySerializer, stringSerializer);
        testOutputGeneratedGroupsFromStreamTopic =
                testDriver.createOutputTopic(O11_groupBy.outputGeneratedGroupsFromStreamTopic,
                        stringDeserializer, longDeserializer);
        testOutputGeneratedGroupsFromTableTopic =
                testDriver.createOutputTopic(O11_groupBy.outputGeneratedGroupsFromTableTopic,
                        stringDeserializer, longDeserializer);
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
                Arrays.asList(  new KeyValue<>("randomKey1".getBytes(), "valueOfType1"),
                                new KeyValue<>("randomKey2".getBytes(), "valueOfType1"),
                                new KeyValue<>("randomKey3".getBytes(), "valueOfType2"),
                                new KeyValue<>("randomKey4".getBytes(), "valueOfType2"),
                                new KeyValue<>("randomKey2".getBytes(), "valueOfType2")
                );
        Map<String, Long> outputKStreamValues = new HashMap<>();
        outputKStreamValues.put("valueOfType1", 2L);
        outputKStreamValues.put("valueOfType2", 3L);

        Map<String, Long> outputKtableValues = new HashMap<>();
        outputKtableValues.put("valueOfType1", 1L);
        outputKtableValues.put("valueOfType2", 3L);

        testInputTopic.pipeKeyValueList(inputValues,
                Instant.ofEpochMilli(0), Duration.ofMillis(100));
        assertThat(testOutputGeneratedGroupsFromStreamTopic.readKeyValuesToMap(), equalTo(outputKStreamValues));
        //No more output in topic
        assertTrue(testOutputGeneratedGroupsFromStreamTopic.isEmpty());

        assertThat(testOutputGeneratedGroupsFromTableTopic.readKeyValuesToMap(), equalTo(outputKtableValues));
        //No more output in topic
        assertTrue(testOutputGeneratedGroupsFromTableTopic.isEmpty());

    }

}
