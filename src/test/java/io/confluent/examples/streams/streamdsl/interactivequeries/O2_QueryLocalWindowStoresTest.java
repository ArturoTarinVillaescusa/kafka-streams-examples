package io.confluent.examples.streams.streamdsl.interactivequeries;

import org.apache.kafka.common.serialization.LongDeserializer;
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
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

/**
 * Stream processing unit test of {@link O2_QueryLocalWindowStores}, using TopologyTestDriver.
 *
 * See {@link O2_QueryLocalWindowStores} for further documentation.
 */
public class O2_QueryLocalWindowStoresTest {
    private static final ZoneId zone = ZoneOffset.UTC;
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> testInputTopic;
    private TestOutputTopic<String, Long> testOutputTopic;
    private TestOutputTopic<String, Long> testOddOutputTopic;
    private TestOutputTopic<String, Long> testPairOutputTopic;
    private StringSerializer stringSerializer = new StringSerializer();
    private StringDeserializer stringDeserializer = new StringDeserializer();
    private LongDeserializer longDeserializer = new LongDeserializer();

    private List<TestRecord<String, String>> inputValues = Arrays.asList(
            new TestRecord<>("key01","This is the text assigned to the key01",
                    ZonedDateTime.of(2020, 1, 1, 16, 35, 59, 0, zone).toInstant())
            ,new TestRecord<>("key02","And this is the one assigned to the key02",
                    ZonedDateTime.of(2020, 1, 1, 16, 36, 01, 0, zone).toInstant())
    );

    @Before
    public void setup() {
        StreamsBuilder builder = new StreamsBuilder();

        // Create actual StreamBuilder topology
        O2_QueryLocalWindowStores.createStream(builder);

        Topology topology = builder.build();

        System.out.println("\n||||||||||||||||||\n\n" + topology.describe() +
                "You can see it in http://zz85.github.io/kafka-streams-viz\n\n" +
                "Alternatively you can run ~/Downloads/apache-tomcat-9.0.39/bin/catalina.sh start\n" +
                "and use your local url http://localhost:8080/kafka-streams-viz/\n" +
                "If you want to play around, save the png graph topology obtained and open it in Chrome url " +
                "https://cloudapps.herokuapp.com/imagetoascii/" +
                "\n||||||||||||||||||\n");

        testDriver = new TopologyTestDriver(topology,
                   O2_QueryLocalWindowStores
                           .getStreamsConfiguration("localhost:9092"));

        testInputTopic =
            testDriver.createInputTopic(O2_QueryLocalWindowStores.inputTopic,
                                        stringSerializer, stringSerializer);
        testOutputTopic =
                testDriver.createOutputTopic(O2_QueryLocalWindowStores.outputTopic,
                        stringDeserializer,longDeserializer);
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
    public void outputStreamReceivesTheExpectedRecords() {
        List<TestRecord<String, Long>> expectedOutputValues = Arrays.asList(
                new TestRecord<>("this", 1L,
                        ZonedDateTime.of(2020, 1, 1, 16, 35, 59, 0, zone).toInstant())
                ,new TestRecord<>("is", 1L,
                        ZonedDateTime.of(2020, 1, 1, 16, 35, 59, 0, zone).toInstant())
                ,new TestRecord<>("the", 1L,
                        ZonedDateTime.of(2020, 1, 1, 16, 35, 59, 0, zone).toInstant())
                ,new TestRecord<>("text", 1L,
                        ZonedDateTime.of(2020, 1, 1, 16, 35, 59, 0, zone).toInstant())
                ,new TestRecord<>("assigned", 1L,
                        ZonedDateTime.of(2020, 1, 1, 16, 35, 59, 0, zone).toInstant())
                ,new TestRecord<>("to", 1L,
                        ZonedDateTime.of(2020, 1, 1, 16, 35, 59, 0, zone).toInstant())
                ,new TestRecord<>("the", 2L,
                        ZonedDateTime.of(2020, 1, 1, 16, 35, 59, 0, zone).toInstant())
                ,new TestRecord<>("key01", 1L,
                        ZonedDateTime.of(2020, 1, 1, 16, 35, 59, 0, zone).toInstant())
                // the records from here arrive in a new window, so although any arriving record has a
                // that already exists in the previous window, the count will be reset
                ,new TestRecord<>("and",1L,
                        ZonedDateTime.of(2020, 1, 1, 16, 36, 01, 0, zone).toInstant())
                ,new TestRecord<>("this",1L,
                        ZonedDateTime.of(2020, 1, 1, 16, 36, 01, 0, zone).toInstant())
                ,new TestRecord<>("is",1L,
                        ZonedDateTime.of(2020, 1, 1, 16, 36, 01, 0, zone).toInstant())
                ,new TestRecord<>("the",1L,
                        ZonedDateTime.of(2020, 1, 1, 16, 36, 01, 0, zone).toInstant())
                ,new TestRecord<>("one",1L,
                        ZonedDateTime.of(2020, 1, 1, 16, 36, 01, 0, zone).toInstant())
                ,new TestRecord<>("assigned",1L,
                        ZonedDateTime.of(2020, 1, 1, 16, 36, 01, 0, zone).toInstant())
                ,new TestRecord<>("to",1L,
                        ZonedDateTime.of(2020, 1, 1, 16, 36, 01, 0, zone).toInstant())
                ,new TestRecord<>("the",2L,
                        ZonedDateTime.of(2020, 1, 1, 16, 36, 01, 0, zone).toInstant())
                ,new TestRecord<>("key02",1L,
                        ZonedDateTime.of(2020, 1, 1, 16, 36, 01, 0, zone).toInstant())
        );

        testInputTopic.pipeRecordList(inputValues);

        testOutputTopic.readRecordsToList().forEach(r -> {
            AtomicBoolean exists = new AtomicBoolean(false);
            expectedOutputValues.forEach(expected -> {
                if (r.getKey().startsWith(expected.getKey()) &&
                        r.getValue().equals(expected.getValue()) &&
                        r.getRecordTime().equals(expected.getRecordTime())) {

                    exists.set(true);
                }
            });
            assertTrue(exists.get());
        });
//        assertThat(testOutputTopic.readRecordsToList(), equalTo(expectedOutputValues));

        //No more output in topic
        assertTrue(testOutputTopic.isEmpty());
    }
}
