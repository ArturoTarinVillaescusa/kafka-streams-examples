package io.confluent.examples.streams.streamdsl.fromstreamtotopic;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Stream processing unit test of {@link O1_UseCustomPartitionerDinamicallyChooseDestTopic}, using TopologyTestDriver.
 *
 * See {@link O1_UseCustomPartitionerDinamicallyChooseDestTopic} for further documentation.
 */
public class O1_UseCustomPartitionerDinamicallyChosenDestTopicTest {
    private static final ZoneId zone = ZoneOffset.UTC;
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> testInputTopic;
    private TestOutputTopic<String, String> testNormalOutputTopic;
    private TestOutputTopic<String, String> testMailOutputTopic;
    private StringSerializer stringSerializer = new StringSerializer();
    private StringDeserializer stringDeserializer = new StringDeserializer();

    private List<TestRecord<String, String>> inputValues = Arrays.asList(
            new TestRecord<>("mailto:arturotarin.villa@hcl.com","value01",
                    ZonedDateTime.of(2020, 1, 1, 04, 33, 0, 0, zone).toInstant())
            ,new TestRecord<>("key02","value02",
                    ZonedDateTime.of(2020, 1, 1, 05, 33, 0, 0, zone).toInstant())
    );

    @Before
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();

        // Create actual StreamBuilder topology
        O1_UseCustomPartitionerDinamicallyChooseDestTopic.createStream(builder);

        Topology topology = builder.build();

        System.out.println("\n||||||||||||||||||\n\n" + topology.describe() +
                "You can see it in http://zz85.github.io/kafka-streams-viz\n\n" +
                "Alternatively you can run ~/Downloads/apache-tomcat-9.0.39/bin/catalina.sh start\n" +
                "and use your local url http://localhost:8080/kafka-streams-viz/\n" +
                "If you want to play around, save the png graph topology obtained and open it in Chrome url " +
                "https://cloudapps.herokuapp.com/imagetoascii/" +
                "\n||||||||||||||||||\n");

        testDriver = new TopologyTestDriver(topology,
                   O1_UseCustomPartitionerDinamicallyChooseDestTopic
                           .getStreamsConfiguration("localhost:9092"));

        testInputTopic =
            testDriver.createInputTopic(O1_UseCustomPartitionerDinamicallyChooseDestTopic.inputTopic,
                                        stringSerializer, stringSerializer);
        testNormalOutputTopic =
            testDriver.createOutputTopic(O1_UseCustomPartitionerDinamicallyChooseDestTopic.normalOutputTopic,
                                        stringDeserializer,stringDeserializer);
        testMailOutputTopic =
            testDriver.createOutputTopic(O1_UseCustomPartitionerDinamicallyChooseDestTopic.mailOutputTopic,
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

    @Test
    public void mailtoKeysMustGoToTheMailTopic() {
        List<TestRecord<String, String>> outputValues = Arrays.asList(
            new TestRecord<>("mailto:arturotarin.villa@hcl.com","value01",
                    ZonedDateTime.of(2020, 1, 1, 04, 33, 0, 0, zone).toInstant())
        );

        testInputTopic.pipeRecordList(inputValues);

        assertThat(testMailOutputTopic.readRecordsToList(), equalTo(outputValues));
        //No more output in topic
        assertTrue(testMailOutputTopic.isEmpty());
    }

    @Test
    public void otherKeysMustGoToTheNormalTopic() {
        List<TestRecord<String, String>> outputValues = Arrays.asList(
                new TestRecord<>("key02","value02",
                        ZonedDateTime.of(2020, 1, 1, 05, 33, 0, 0, zone).toInstant())
        );

        testInputTopic.pipeRecordList(inputValues);

        assertThat(testNormalOutputTopic.readRecordsToList(), equalTo(outputValues));
        //No more output in topic
        assertTrue(testNormalOutputTopic.isEmpty());
    }

}
