package io.confluent.examples.streams.streamdsl.stateful.aggregating;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
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
 * Stream processing unit test of {@link O07_windowedByCount}, using TopologyTestDriver.
 *
 * See {@link O07_windowedByCount} for further documentation.
 */
public class O07_windowedByCountTest {
    private static final ZoneId zone = ZoneOffset.UTC;
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Long> testInputTopic;
    private TestOutputTopic<String, Long> testTimeWindowedCountOutputTopic;
    private TestOutputTopic<String, Long> testSessionWindowedCountOutputTopic;
    private StringSerializer stringSerializer = new StringSerializer();
    private LongSerializer longSerializer = new LongSerializer();
    private StringDeserializer stringDeserializer = new StringDeserializer();
    private LongDeserializer longDeserializer = new LongDeserializer();

    @Before
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();

        // Create actual StreamBuilder topology
        O07_windowedByCount.createStream(builder);

        Topology topology = builder.build();

        System.out.println("\n||||||||||||||||||\n\n" + topology.describe() +
                "You can see it in http://zz85.github.io/kafka-streams-viz\n\n" +
                "Alternatively you can run ~/Downloads/apache-tomcat-9.0.39/bin/catalina.sh start\n" +
                "and use your local url http://localhost:8080/kafka-streams-viz/\n" +
                "If you want to play around, save the png graph topology obtained and open it in Chrome url " +
                "https://cloudapps.herokuapp.com/imagetoascii/" +
                "\n||||||||||||||||||\n");

        testDriver = new TopologyTestDriver(topology,
                               O07_windowedByCount.getStreamsConfiguration("localhost:9092"));

        testInputTopic =
                testDriver.createInputTopic(O07_windowedByCount.inputTopic,
                                            stringSerializer, longSerializer);
        testTimeWindowedCountOutputTopic =
                testDriver.createOutputTopic(O07_windowedByCount.timeWindowedCountOutTopic,
                        stringDeserializer,longDeserializer);
        testSessionWindowedCountOutputTopic =
                testDriver.createOutputTopic(O07_windowedByCount.sessionWindowedCountOutTopic,
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

    /**
     *  Simple test validating several input records
     */
    @Test
    public void outputTopicMustContainCountedByWindowInputTopicRecords() {
        List<TestRecord<String, Long>> inputValues =
            Arrays.asList(
                new TestRecord<>("key01",1L,
                    ZonedDateTime.of(2020, 1, 1, 16, 29, 0, 0, zone).toInstant())
                ,new TestRecord<>("key01",13535L,
                    ZonedDateTime.of(2020, 1, 1, 16, 29, 10, 0, zone).toInstant())
                ,new TestRecord<>("key03",1L,
                    ZonedDateTime.of(2020, 1, 1, 16, 31, 22, 0, zone).toInstant())
                ,new TestRecord<>("key03",23L,
                    ZonedDateTime.of(2020, 1, 1, 16, 31, 40, 0, zone).toInstant())
                ,new TestRecord<>("key02",2342L,
                    ZonedDateTime.of(2020, 1, 1, 16, 32, 03, 0, zone).toInstant())
                ,new TestRecord<>("key01",21L,
                    ZonedDateTime.of(2020, 1, 1, 16, 33, 0, 0, zone).toInstant())
                ,new TestRecord<>("key02",117L,
                    ZonedDateTime.of(2020, 1, 1, 16, 33, 30, 0, zone).toInstant())
                ,new TestRecord<>("key01",22L,
                    ZonedDateTime.of(2020, 1, 1, 16, 34, 21, 0, zone).toInstant())
                ,new TestRecord<>("key04",2L,
                    ZonedDateTime.of(2020, 1, 1, 16, 35, 59, 0, zone).toInstant())
                ,new TestRecord<>("key02",12L,
                    ZonedDateTime.of(2020, 1, 1, 16, 39, 0, 0, zone).toInstant())
            );

        final List<TestRecord<String, Long>>  expectedTimeWindowedCountOutValues = Arrays.asList(
             new TestRecord<>("key01",1L,
                ZonedDateTime.of(2020, 1, 1, 16, 29, 0, 0, zone).toInstant())
            ,new TestRecord<>("key01",2L,
                ZonedDateTime.of(2020, 1, 1, 16, 29, 10, 0, zone).toInstant())
            // This record arrives outside the previous Window, so a new count window is created
            // starting with it's value
            ,new TestRecord<>("key01",1L,
                ZonedDateTime.of(2020, 1, 1, 16, 33, 0, 0, zone).toInstant())
            ,new TestRecord<>("key01",2L,
                ZonedDateTime.of(2020, 1, 1, 16, 34, 21, 0, zone).toInstant())

            ,new TestRecord<>("key02",1L,
                ZonedDateTime.of(2020, 1, 1, 16, 32, 03, 0, zone).toInstant())
            ,new TestRecord<>("key02",2L,
                ZonedDateTime.of(2020, 1, 1, 16, 33, 30, 0, zone).toInstant())
            // This record arrives outside the previous Window, so a new count window is created
            // starting with it's value
            ,new TestRecord<>("key02",1L,
                ZonedDateTime.of(2020, 1, 1, 16, 39, 0, 0, zone).toInstant())
            ,new TestRecord<>("key02",null,
                ZonedDateTime.of(2020, 1, 1, 16, 39, 0, 0, zone).toInstant())
            ,new TestRecord<>("key02",2L,
                ZonedDateTime.of(2020, 1, 1, 16, 32, 03, 0, zone).toInstant())

            ,new TestRecord<>("key03",1L,
                ZonedDateTime.of(2020, 1, 1, 16, 31, 22, 0, zone).toInstant())
            ,new TestRecord<>("key03",2L,
                ZonedDateTime.of(2020, 1, 1, 16, 31, 40, 0, zone).toInstant())

            ,new TestRecord<>("key04",1L,
                ZonedDateTime.of(2020, 1, 1, 16, 35, 59, 0, zone).toInstant())
        );

        final List<TestRecord<String, Long>>  expectedSessionWindowedCountOutValues = Arrays.asList(
             new TestRecord<>("key01",1L,
                ZonedDateTime.of(2020, 1, 1, 16, 29, 0, 0, zone).toInstant())
            ,new TestRecord<>("key01",null,
                ZonedDateTime.of(2020, 1, 1, 16, 29, 0, 0, zone).toInstant())
            ,new TestRecord<>("key01",2L,
                    ZonedDateTime.of(2020, 1, 1, 16, 29, 10, 0, zone).toInstant())
            ,new TestRecord<>("key01",null,
                    ZonedDateTime.of(2020, 1, 1, 16, 29, 10, 0, zone).toInstant())
            ,new TestRecord<>("key01",3L,
                    ZonedDateTime.of(2020, 1, 1, 16, 33, 0, 0, zone).toInstant())
            ,new TestRecord<>("key01",null,
                    ZonedDateTime.of(2020, 1, 1, 16, 33, 0, 0, zone).toInstant())
            ,new TestRecord<>("key01",4L,
                    ZonedDateTime.of(2020, 1, 1, 16, 34, 21, 0, zone).toInstant())

            ,new TestRecord<>("key02",1L,
                    ZonedDateTime.of(2020, 1, 1, 16, 32, 03, 0, zone).toInstant())
            ,new TestRecord<>("key02",null,
                    ZonedDateTime.of(2020, 1, 1, 16, 32, 03, 0, zone).toInstant())
            ,new TestRecord<>("key02",2L,
                    ZonedDateTime.of(2020, 1, 1, 16, 33, 30, 0, zone).toInstant())
            ,new TestRecord<>("key02",null,
                    ZonedDateTime.of(2020, 1, 1, 16, 33, 30, 0, zone).toInstant())
            ,new TestRecord<>("key02",1L,
                    ZonedDateTime.of(2020, 1, 1, 16, 39, 0, 0, zone).toInstant())

            ,new TestRecord<>("key03",1L,
                ZonedDateTime.of(2020, 1, 1, 16, 31, 22, 0, zone).toInstant())
            ,new TestRecord<>("key03",null,
                ZonedDateTime.of(2020, 1, 1, 16, 31, 22, 0, zone).toInstant())
            ,new TestRecord<>("key03",2L,
                ZonedDateTime.of(2020, 1, 1, 16, 31, 40, 0, zone).toInstant())

            ,new TestRecord<>("key04",1L,
                    ZonedDateTime.of(2020, 1, 1, 16, 35, 59, 0, zone).toInstant())

        );

        testInputTopic.pipeRecordList(inputValues);

        List<TestRecord<String, Long>> timeWindowedCountOutputRecordsList =
                testTimeWindowedCountOutputTopic.readRecordsToList();

        timeWindowedCountOutputRecordsList.forEach(r -> {
            AtomicBoolean exists = new AtomicBoolean(false);
            expectedTimeWindowedCountOutValues.forEach(expected -> {
                if (r.getKey().startsWith(expected.getKey()) &&
                    r.getValue().equals(expected.getValue()) &&
                    r.getRecordTime().equals(expected.getRecordTime())) {

                    exists.set(true);
                }
            });
            assertTrue(exists.get());
        });
            //No more output in topic
        assertTrue(testTimeWindowedCountOutputTopic.isEmpty());

        List<TestRecord<String, Long>> sessionWindowedCountOutputRecordsList =
                testSessionWindowedCountOutputTopic.readRecordsToList();

        sessionWindowedCountOutputRecordsList.forEach(r -> {
            AtomicBoolean exists = new AtomicBoolean(false);
            expectedSessionWindowedCountOutValues.forEach(expected -> {
                if (r.getKey().startsWith(expected.getKey()) &&
                    areTheyEqual(r.getValue(), expected.getValue()) &&
                    r.getRecordTime().equals(expected.getRecordTime())) {

                    exists.set(true);
                }
            });
            assertTrue(exists.get());
        });

        //No more output in topic
        assertTrue(testSessionWindowedCountOutputTopic.isEmpty());
    }

    private boolean areTheyEqual(Long lv, Long rv) {
        boolean result;
        if (lv == null || rv == null) {
            result = lv == rv;
        } else {
            result = lv.equals(rv);
        }
        return result;
    }
}
