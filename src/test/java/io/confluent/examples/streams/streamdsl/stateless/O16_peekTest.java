package io.confluent.examples.streams.streamdsl.stateless;

import io.confluent.examples.streams.streamdsl.stateless.O16_peek;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Stream processing unit test of {@link O16_peek}, using TopologyTestDriver.
 *
 * See {@link O16_peek} for further documentation.
 */
public class O16_peekTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<byte[], String> testInputTopic1;
    private TestInputTopic<byte[], String> testInputTopic2;
    private TestOutputTopic<byte[], String> testOutputTopic;
    private StringSerializer stringSerializer = new StringSerializer();
    private ByteArraySerializer byteArraySerializer = new ByteArraySerializer();
    private StringDeserializer stringDeserializer = new StringDeserializer();
    private ByteArrayDeserializer byteArrayDeserializer = new ByteArrayDeserializer();

    @Before
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();

        // Create actual StreamBuilder topology
        O16_peek.createStream(builder);

        Topology topology = builder.build();

        System.out.println("\n||||||||||||||||||\n\n" + topology.describe() +
                "You can see it in http://zz85.github.io/kafka-streams-viz\n\n" +
                "Alternatively you can run ~/Downloads/apache-tomcat-9.0.39/bin/catalina.sh start\n" +
                "and use your local url http://localhost:8080/kafka-streams-viz/\n" +
                "If you want to play around, save the png graph topology obtained and open it in Chrome url " +
                "https://cloudapps.herokuapp.com/imagetoascii/" +
                "\n||||||||||||||||||\n");

        testDriver = new TopologyTestDriver(topology,
                               O16_peek.getStreamsConfiguration("localhost:9092"));

        testInputTopic1 =
                testDriver.createInputTopic(O16_peek.inputTopic1,
                        byteArraySerializer, stringSerializer);
        testInputTopic2 =
                testDriver.createInputTopic(O16_peek.inputTopic2,
                        byteArraySerializer, stringSerializer);
        testOutputTopic =
                testDriver.createOutputTopic(O16_peek.outputTopic,
                        byteArrayDeserializer,stringDeserializer);
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
        List<KeyValue<byte[], String>> inputValues1 =
                Arrays.asList(
                        new KeyValue<>("Hello".getBytes(), "World!"),
                        new KeyValue<>("How are".getBytes(), "you?")
                );
        List<KeyValue<byte[], String>> inputValues2 =
                Arrays.asList(
                        new KeyValue<>("I'm fine,".getBytes(), "thank you.")
                );

        List<String> expectedOutput = new ArrayList<>();
        expectedOutput.add("World!");
        expectedOutput.add("you?");
        expectedOutput.add("thank you.");

        testInputTopic1.pipeKeyValueList(inputValues1,
                Instant.ofEpochMilli(0), Duration.ofMillis(100));
        testInputTopic2.pipeKeyValueList(inputValues2,
                Instant.ofEpochMilli(0), Duration.ofMillis(100));

        assertThat(testOutputTopic.readValuesToList(), equalTo(expectedOutput));
        //No more output in topic
        assertTrue(testOutputTopic.isEmpty());
    }

}
