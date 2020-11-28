package io.confluent.examples.streams.streamdsl.stateful.windowing;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.test.TestRecord;
import org.hamcrest.core.IsEqual;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.*;

import static io.confluent.examples.streams.streamdsl.stateful.windowing.O01_tumblingWindow.MOCK_SCHEMA_REGISTRY_URL;
import static io.confluent.examples.streams.streamdsl.stateful.windowing.O01_tumblingWindow.zone;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Stream processing unit test of {@link O05_windowFinalResults_ThresholdReached}, using TopologyTestDriver.
 *
 * See {@link O05_windowFinalResults_ThresholdReached} for further documentation.
 */
public class O05_windowFinalResults_ThresholdReachedTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, GenericRecord> testInputTopic;
    private TestOutputTopic<String, Long> testCountTableOutputTopic;
    private StringSerializer stringSerializer = new StringSerializer();
    private IntegerSerializer intSerializer = new IntegerSerializer();
    private StringDeserializer stringDeserializer = new StringDeserializer();
    private LongDeserializer longDeserializer = new LongDeserializer();
    private Serde<GenericRecord> avroSerde = new GenericAvroSerde();

    @Before
    public void setup() {
        final Map<String,String> avroSerdeConfig = new HashMap<>();
        avroSerdeConfig.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        avroSerde.configure(avroSerdeConfig, false);

        final StreamsBuilder builder = new StreamsBuilder();

        // Create actual StreamBuilder topology
        O05_windowFinalResults_ThresholdReached.createStream(builder);

        Topology topology = builder.build();

        System.out.println("\n||||||||||||||||||\n\n" + topology.describe() +
                "You can see it in http://zz85.github.io/kafka-streams-viz\n\n" +
                "Alternatively you can run ~/Downloads/apache-tomcat-9.0.39/bin/catalina.sh start\n" +
                "and use your local url http://localhost:8080/kafka-streams-viz/\n" +
                "If you want to play around, save the png graph topology obtained and open it in Chrome url " +
                "https://cloudapps.herokuapp.com/imagetoascii/" +
                "\n||||||||||||||||||\n");

        testDriver = new TopologyTestDriver(topology,
                               O05_windowFinalResults_ThresholdReached.getStreamsConfiguration("localhost:9092"));

        testInputTopic =
                testDriver.createInputTopic(O05_windowFinalResults_ThresholdReached.inputTopic,
                                            stringSerializer, avroSerde.serializer());
        testCountTableOutputTopic =
                testDriver.createOutputTopic(O05_windowFinalResults_ThresholdReached.countTableOutputTopic,
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
    public void shouldCountDifferentRecordKeysSeparatelyInSeparateWindowBlocks() throws IOException, RestClientException {

        final Random random = new Random();

        // Stream 1 of metric events
        List<TestRecord<String, GenericRecord>> inputValues1 = Arrays.asList(
                   // metric1 events < 3. Will not trigger an alert
                    new TestRecord<>("metric01",
                             createGenericRecord("metric01", String.valueOf(random.nextInt(1000)), String.valueOf(System.currentTimeMillis())),
                             ZonedDateTime.of(2020, 3, 30, 0, 0, 20, 0, zone).toInstant()
                    ),
                    new TestRecord<>("metric01",
                            createGenericRecord("metric01", String.valueOf(random.nextInt(1000)), String.valueOf(System.currentTimeMillis())),
                            ZonedDateTime.of(2020, 3, 30, 0, 2, 10, 0, zone).toInstant()
                    ),
                    // metric1 events >= 3 in a window of 5 minutes + 10 seconds of period grace:
                    // WILL TRIGGER AN ALERT
                    new TestRecord<>("metric02",
                            createGenericRecord("metric02", String.valueOf(random.nextInt(1000)), String.valueOf(System.currentTimeMillis())),
                            ZonedDateTime.of(2020, 3, 30, 0, 3, 14, 0, zone).toInstant()
                    ),
                    new TestRecord<>("metric02",
                            createGenericRecord("metric02", String.valueOf(random.nextInt(1000)), String.valueOf(System.currentTimeMillis())),
                            ZonedDateTime.of(2020, 3, 30, 0, 4, 30, 0, zone).toInstant()
                    ),
                    new TestRecord<>("metric02",
                            createGenericRecord("metric02", String.valueOf(random.nextInt(1000)), String.valueOf(System.currentTimeMillis())),
                            ZonedDateTime.of(2020, 3, 30, 0, 4, 50, 0, zone).toInstant()
                    ),
                    flushInputValuesToTopic()
                );

        // Stream 2 of metric events
        List<TestRecord<String, GenericRecord>> inputValues2 = Arrays.asList(
                // metric03 events < 3 in a window of 5 minutes + grace period of 10 seconds:
                // Will not trigger an alert
                new TestRecord<>("metric03",
                        createGenericRecord("metric03", String.valueOf(random.nextInt(1000)), String.valueOf(System.currentTimeMillis())),
                        ZonedDateTime.of(2020, 3, 30, 0, 0, 22, 0, zone).toInstant()
                ),
                new TestRecord<>("metric03",
                        createGenericRecord("metric03", String.valueOf(random.nextInt(1000)), String.valueOf(System.currentTimeMillis())),
                        ZonedDateTime.of(2020, 3, 30, 0, 4, 30, 0, zone).toInstant()
                ),
                new TestRecord<>("metric03",
                        createGenericRecord("metric03", String.valueOf(random.nextInt(1000)), String.valueOf(System.currentTimeMillis())),
                        ZonedDateTime.of(2020, 3, 30, 0, 8, 44, 0, zone).toInstant()
                ),
                // metric1 events < 3. Will not trigger an alert
                new TestRecord<>("metric04",
                        createGenericRecord("metric04", String.valueOf(random.nextInt(1000)), String.valueOf(System.currentTimeMillis())),
                        ZonedDateTime.of(2020, 3, 30, 0, 3, 24, 0, zone).toInstant()
                ),
                new TestRecord<>("metric04",
                        createGenericRecord("metric04", String.valueOf(random.nextInt(1000)), String.valueOf(System.currentTimeMillis())),
                        ZonedDateTime.of(2020, 3, 30, 0, 6, 39, 0, zone).toInstant()
                ),
                flushInputValuesToTopic()
        );

        Map<String, Long> expectedOutputMappedValues = new HashMap<>();
        expectedOutputMappedValues.put("metric02", 3L);

        testInputTopic.pipeRecordList(inputValues1);
        testInputTopic.pipeRecordList(inputValues2);

        theTopologyMustProduce(expectedOutputMappedValues);
    }

    private void theTopologyMustProduce(Map<String, Long> expectedOutputMappedValues) {
        testCountTableOutputTopic.readRecordsToList().forEach(kv -> {
                if (kv.getValue() != null)
                    assertThat(kv.getValue(),
                               IsEqual.equalTo(expectedOutputMappedValues.get(kv.getKey().substring(0, 8))));
            }
        );

        // After consuming all the records from the output topic, no more records must exist in the output in topic
        assertTrue(testCountTableOutputTopic.isEmpty());
    }

    public static GenericRecord createGenericRecord(String metricId, String metricValue, String webSite) {
        EventMock eventMock = new EventMock();
        eventMock.metricId = metricId;
        eventMock.metricValue = metricValue;
        eventMock.webSite = webSite;

        return createGenericRecordFromObject(eventMock);
    }


    private static GenericRecord createGenericRecordFromObject(Object object) {
        final Schema schema = ReflectData.get().getSchema(object.getClass());
        final GenericData.Record record = new GenericData.Record(schema);
        Arrays.stream(object.getClass().getDeclaredFields()).forEach(field -> {
            try {
                record.put(field.getName(), field.get(object));
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        });
        return record;
    }

    private static class EventMock {
        String webSite;
        String metricId;
        String metricValue;
    }

    /** Generates an event after window end + grace period to trigger flush everything through suppression
     @see KTable#suppress(Suppressed)
     */
    private TestRecord<String, GenericRecord> flushInputValuesToTopic() {
        return new TestRecord<>(null,
                createGenericRecord("", "", ""),
                ZonedDateTime.now().toInstant());
    }

}
