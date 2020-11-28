package io.confluent.examples.streams.streamdsl.ktableemitrate;

import io.confluent.examples.streams.streamdsl.stateful.aggregating.O07_windowedByCount;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.confluent.examples.streams.streamdsl.ktableemitrate.O01_windowedByCountWithKTableEmitRate.getStreamsConfiguration;
import static org.junit.Assert.assertTrue;

/**
 * Stream processing unit test of {@link O01_windowedByCountWithKTableEmitRate}, using TopologyTestDriver.
 *
 * See {@link O01_windowedByCountWithKTableEmitRate} for further documentation.
 */
@Slf4j
public class O01_windowedByCountWithKtableEmitRateTest {
    private static final ZoneId zone = ZoneOffset.UTC;
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Long> testInputTopic;
    private TestOutputTopic<String, Long> testTimeWindowedCountOutputTopic;
    private TestOutputTopic<Windowed<String>, Long> testSessionWindowedCountOutputTopic;
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
                               getStreamsConfiguration("localhost:9092"));

        testInputTopic =
                testDriver.createInputTopic(O01_windowedByCountWithKTableEmitRate.inputTopic,
                        stringSerializer, longSerializer);
        testTimeWindowedCountOutputTopic =
                testDriver.createOutputTopic(O01_windowedByCountWithKTableEmitRate.timeWindowedCountOutTopic,
                        stringDeserializer,longDeserializer);
        testSessionWindowedCountOutputTopic =
                testDriver.createOutputTopic(O01_windowedByCountWithKTableEmitRate.sessionWindowedCountOutTopic,
                        new TimeWindowedDeserializer<>(
                                stringDeserializer,
                                Duration.ofDays(1).toMillis()),
                        longDeserializer);
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
    public void outputTopicMustCountWindowedAccordingWithKTableEmitRate() {

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
            // New arriving key01 are aggregated in a new window
            ,new TestRecord<>("key01",1L,
                    ZonedDateTime.of(2020, 1, 1, 16, 33, 0, 0, zone).toInstant())
            ,new TestRecord<>("key01",2L,
                    ZonedDateTime.of(2020, 1, 1, 16, 34, 21, 0, zone).toInstant())

            ,new TestRecord<>("key02",1L,
                    ZonedDateTime.of(2020, 1, 1, 16, 32, 03, 0, zone).toInstant())
            ,new TestRecord<>("key02",2L,
                    ZonedDateTime.of(2020, 1, 1, 16, 33, 30, 0, zone).toInstant())
            // New arriving key02 are aggregated in a new window
            ,new TestRecord<>("key02",1L,
                    ZonedDateTime.of(2020, 1, 1, 16, 39, 0, 0, zone).toInstant())

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
                // Re-setting key02 counter as is arriving in a new session (> 5 minutes of inactivity)
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

        List<TestRecord<String, Long>> timeWindowedOutputRecordsList =
                testTimeWindowedCountOutputTopic.readRecordsToList();

        timeWindowedOutputRecordsList.forEach(r -> {
            AtomicBoolean exists = new AtomicBoolean(false);

            expectedTimeWindowedCountOutValues.forEach(expectedRecord -> {
                if (r.getKey().startsWith(expectedRecord.getKey()) &&
                    r.getValue().equals(expectedRecord.getValue()) &&
                    r.getRecordTime().equals(expectedRecord.getRecordTime())) {

                    exists.set(true);
                }
            });
            assertTrue(exists.get());
        });

        //No more output in topic
        assertTrue(testTimeWindowedCountOutputTopic.isEmpty());

        List<TestRecord<Windowed<String>, Long>> sessionWindowedOutputRecordsList =
                testSessionWindowedCountOutputTopic.readRecordsToList();

        sessionWindowedOutputRecordsList.forEach(r-> {

            log.info("r.getKey()....{}\nr.getKey().key()....{}\nr.getKey().window().start()....{}\nr.getKey().window().end()....{}\nr.value()....{}\n",
                    r.getKey(), r.getKey().key(), r.getKey().window().start(), r.getKey().window().end(), r.value());
            AtomicBoolean exists = new AtomicBoolean(false);
            expectedSessionWindowedCountOutValues.forEach(expectedRecord -> {
                if (r.getKey().key().startsWith(expectedRecord.getKey()) &&
                        r.getValue() == expectedRecord.getValue() &&
                        r.getRecordTime().equals(expectedRecord.getRecordTime())) {

                    exists.set(true);
                }
            });
            assertTrue(exists.get());
        });

        //No more output in topic
        assertTrue(testSessionWindowedCountOutputTopic.isEmpty());
    }

}
