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
 * Stream processing unit test of {@link O10_windowedByReduce}, using TopologyTestDriver.
 *
 * See {@link O10_windowedByReduce} for further documentation.
 */
public class O10_windowedByReduceTest {
    private static final ZoneId zone = ZoneOffset.UTC;
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Long> testInputTopic;
    private TestOutputTopic<String, Long> testTimeWindowedReduceOutputTopic;
    private TestOutputTopic<String, Long> testSessionWindowedReduceOutputTopic;
    private StringSerializer stringSerializer = new StringSerializer();
    private LongSerializer longSerializer = new LongSerializer();
    private StringDeserializer stringDeserializer = new StringDeserializer();
    private LongDeserializer longDeserializer = new LongDeserializer();

    @Before
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();

        // Create actual StreamBuilder topology
        O10_windowedByReduce.createStream(builder);

        Topology topology = builder.build();

        System.out.println("\n||||||||||||||||||\n\n" + topology.describe() +
                "You can see it in http://zz85.github.io/kafka-streams-viz\n\n" +
                "Alternatively you can run ~/Downloads/apache-tomcat-9.0.39/bin/catalina.sh start\n" +
                "and use your local url http://localhost:8080/kafka-streams-viz/\n" +
                "If you want to play around, save the png graph topology obtained and open it in Chrome url " +
                "https://cloudapps.herokuapp.com/imagetoascii/" +
                "\n||||||||||||||||||\n");

        testDriver = new TopologyTestDriver(topology,
                               O10_windowedByReduce.getStreamsConfiguration("localhost:9092"));

        testInputTopic =
                testDriver.createInputTopic(O10_windowedByReduce.inputTopic,
                                            stringSerializer, longSerializer);
        testTimeWindowedReduceOutputTopic =
                testDriver.createOutputTopic(O10_windowedByReduce.timeWindowedReduceOutTopic,
                        stringDeserializer,longDeserializer);
        testSessionWindowedReduceOutputTopic =
                testDriver.createOutputTopic(O10_windowedByReduce.sessionWindowedReduceOutTopic,
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
    public void outputTopicMustContainReducedByWindowRecords() {
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

        final List<TestRecord<String, Long>>  expectedTimeWindowedSumOutValues = Arrays.asList(
                new TestRecord<>("key01",1L,
                        ZonedDateTime.of(2020, 1, 1, 16, 29, 0, 0, zone).toInstant())
                ,new TestRecord<>("key01",13536L,
                        ZonedDateTime.of(2020, 1, 1, 16, 29, 10, 0, zone).toInstant())
                // The new arriving key01 falls out of the previous Time Window, meaning the sum of key01
                // is started again with this new value in a new Time Window
                ,new TestRecord<>("key01",21L,
                        ZonedDateTime.of(2020, 1, 1, 16, 33, 0, 0, zone).toInstant())
                ,new TestRecord<>("key01",43L,
                        ZonedDateTime.of(2020, 1, 1, 16, 34, 21, 0, zone).toInstant())

                ,new TestRecord<>("key02",2342L,
                        ZonedDateTime.of(2020, 1, 1, 16, 32, 03, 0, zone).toInstant())
                ,new TestRecord<>("key02",2459L,
                        ZonedDateTime.of(2020, 1, 1, 16, 33, 30, 0, zone).toInstant())
                // The new arriving key02 falls in a new Time Window, meaning the sum of key02 is reset
                ,new TestRecord<>("key02",12L,
                        ZonedDateTime.of(2020, 1, 1, 16, 39, 0, 0, zone).toInstant())

                ,new TestRecord<>("key03",1L,
                        ZonedDateTime.of(2020, 1, 1, 16, 31, 22, 0, zone).toInstant())
                ,new TestRecord<>("key03",24L,
                        ZonedDateTime.of(2020, 1, 1, 16, 31, 40, 0, zone).toInstant())

                ,new TestRecord<>("key04",2L,
                        ZonedDateTime.of(2020, 1, 1, 16, 35, 59, 0, zone).toInstant())
        );

        final List<TestRecord<String, Long>>  expectedSessionWindowedSumOutValues = Arrays.asList(
                new TestRecord<>("key01",1L,
                        ZonedDateTime.of(2020, 1, 1, 16, 29, 0, 0, zone).toInstant())
               ,new TestRecord<>("key01",null,
                        ZonedDateTime.of(2020, 1, 1, 16, 29, 0, 0, zone).toInstant())
               ,new TestRecord<>("key01",13536L,
                        ZonedDateTime.of(2020, 1, 1, 16, 29, 10, 0, zone).toInstant())
               ,new TestRecord<>("key01",null,
                        ZonedDateTime.of(2020, 1, 1, 16, 29, 10, 0, zone).toInstant())
               ,new TestRecord<>("key01",13557L,
                        ZonedDateTime.of(2020, 1, 1, 16, 33, 0, 0, zone).toInstant())
                ,new TestRecord<>("key01",null,
                        ZonedDateTime.of(2020, 1, 1, 16, 33, 0, 0, zone).toInstant())
               ,new TestRecord<>("key01",13579L,
                        ZonedDateTime.of(2020, 1, 1, 16, 34, 21, 0, zone).toInstant())

                ,new TestRecord<>("key02",2342L,
                        ZonedDateTime.of(2020, 1, 1, 16, 32, 03, 0, zone).toInstant())
                ,new TestRecord<>("key02",null,
                        ZonedDateTime.of(2020, 1, 1, 16, 32, 03, 0, zone).toInstant())
                ,new TestRecord<>("key02",2459L,
                        ZonedDateTime.of(2020, 1, 1, 16, 33, 30, 0, zone).toInstant())
                ,new TestRecord<>("key02",null,
                        ZonedDateTime.of(2020, 1, 1, 16, 33, 30, 0, zone).toInstant())
                // The new arriving key02 falls in a new Time Window, meaning the sum of key02 is reset
                ,new TestRecord<>("key02",12L,
                        ZonedDateTime.of(2020, 1, 1, 16, 39, 0, 0, zone).toInstant())

                ,new TestRecord<>("key03",1L,
                        ZonedDateTime.of(2020, 1, 1, 16, 31, 22, 0, zone).toInstant())
                ,new TestRecord<>("key03",null,
                        ZonedDateTime.of(2020, 1, 1, 16, 31, 22, 0, zone).toInstant())
                ,new TestRecord<>("key03",24L,
                        ZonedDateTime.of(2020, 1, 1, 16, 31, 40, 0, zone).toInstant())

                ,new TestRecord<>("key04",2L,
                        ZonedDateTime.of(2020, 1, 1, 16, 35, 59, 0, zone).toInstant())
        );

        testInputTopic.pipeRecordList(inputValues);

        List<TestRecord<String, Long>> timeWindowedOutputRecordsList =
                testTimeWindowedReduceOutputTopic.readRecordsToList();

        timeWindowedOutputRecordsList.forEach(r -> {
            AtomicBoolean exists = new AtomicBoolean(false);
            expectedTimeWindowedSumOutValues.forEach(expected -> {
                if (r.getKey().startsWith(expected.getKey()) &&
                        r.getValue().equals(expected.getValue()) &&
                        r.getRecordTime().equals(expected.getRecordTime())) {

                    exists.set(true);
                }
            });
            assertTrue(exists.get());
        });

        //No more output in topic
        assertTrue(testTimeWindowedReduceOutputTopic.isEmpty());

        List<TestRecord<String, Long>> sessionWindowedOutputRecordsList =
                testSessionWindowedReduceOutputTopic.readRecordsToList();

        sessionWindowedOutputRecordsList.forEach(r -> {
            AtomicBoolean exists = new AtomicBoolean(false);
            expectedSessionWindowedSumOutValues.forEach(expected -> {
                if (r.getKey().startsWith(expected.getKey()) &&
                    areTheyEqual(r.getValue(), expected.getValue()) &&
                    r.getRecordTime().equals(expected.getRecordTime())) {

                    exists.set(true);
                }
            });
            assertTrue(exists.get());
        });
        //No more output in topic
        assertTrue(testSessionWindowedReduceOutputTopic.isEmpty());

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
