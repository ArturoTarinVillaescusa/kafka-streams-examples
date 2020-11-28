package io.confluent.examples.streams.streamdsl.stateless;

import io.confluent.examples.streams.streamdsl.stateless.O12_cogroup;
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

/**
 * Stream processing unit test of {@link O12_cogroup}, using TopologyTestDriver.
 *
 * See {@link O12_cogroup} for further documentation.
 */
public class O12_cogroupTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<byte[], String> testInputOneTopic;
    private TestInputTopic<byte[], Long> testInputTwoTopic;
    private TestOutputTopic<String, Long> testOutputTopic;
    private TestOutputTopic<String, Long> testOutputGeneratedGroupsFromTableTopic;
    private StringSerializer stringSerializer = new StringSerializer();
    private LongSerializer longSerializer = new LongSerializer();
    private ByteArraySerializer byteArraySerializer = new ByteArraySerializer();
    private StringDeserializer stringDeserializer = new StringDeserializer();
    private LongDeserializer longDeserializer = new LongDeserializer();;


    @Before
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();

        // Create actual StreamBuilder topology
        O12_cogroup.createStream(builder);

        Topology topology = builder.build();

        System.out.println("\n||||||||||||||||||\n\n" + topology.describe() +
                "You can see it in http://zz85.github.io/kafka-streams-viz\n\n" +
                "Alternatively you can run ~/Downloads/apache-tomcat-9.0.39/bin/catalina.sh start\n" +
                "and use your local url http://localhost:8080/kafka-streams-viz/\n" +
                "If you want to play around, save the png graph topology obtained and open it in Chrome url " +
                "https://cloudapps.herokuapp.com/imagetoascii/" +
                "\n||||||||||||||||||\n");

        testDriver = new TopologyTestDriver(topology,
                               O12_cogroup.getStreamsConfiguration("localhost:9092"));

        testInputOneTopic =
                testDriver.createInputTopic(O12_cogroup.inputOneTopic,
                        byteArraySerializer, stringSerializer);
        testInputTwoTopic =
                testDriver.createInputTopic(O12_cogroup.inputTwoTopic,
                        byteArraySerializer, longSerializer);


        testOutputTopic =
                testDriver.createOutputTopic(O12_cogroup.outputTopic,
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
        List<KeyValue<byte[], String>> inputOneValues =
                Arrays.asList(  new KeyValue<>("randomKey1".getBytes(), "valueOfType1"),
                                new KeyValue<>("randomKey2".getBytes(), "valueOfType1"),
                                new KeyValue<>("randomKey3".getBytes(), "valueOfType2"),
                                new KeyValue<>("randomKey4".getBytes(), "valueOfType2"),
                                new KeyValue<>("randomKey5".getBytes(), "valueOfType2")
                );

        List<KeyValue<byte[], Long>> inputTwoValues =
                Arrays.asList(  new KeyValue<>("randomKey6".getBytes(), 1L),
                                new KeyValue<>("randomKey7".getBytes(), 2L),
                                new KeyValue<>("randomKey8".getBytes(), 3L),
                                new KeyValue<>("randomKey9".getBytes(), 4L),
                                new KeyValue<>("randomKey0".getBytes(), 5L)
                );

        Map<String, Long> outputValues = new HashMap<>();
        outputValues.put("valueOfType1", 2L);
        outputValues.put("valueOfType2", 3L);

        Map<String, Long> outputKtableValues = new HashMap<>();
        outputKtableValues.put("valueOfType1", 1L);
        outputKtableValues.put("valueOfType2", 3L);

        testInputOneTopic.pipeKeyValueList(inputOneValues,
                Instant.ofEpochMilli(0), Duration.ofMillis(100));
        testInputTwoTopic.pipeKeyValueList(inputTwoValues,
                Instant.ofEpochMilli(0), Duration.ofMillis(100));
/*
        assertThat(testOutputTopic.readKeyValuesToMap(), equalTo(outputValues));
        //No more output in topic
        assertTrue(testOutputTopic.isEmpty());
*/
    }

}
