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
 * Stream processing unit test of {@link O04_sessionWindow}, using TopologyTestDriver.
 *
 * See {@link O04_sessionWindow} for further documentation.
 */
public class O04_sessionWindowTest {

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
        O04_sessionWindow.createStream(builder);

        Topology topology = builder.build();

        System.out.println("\n||||||||||||||||||\n\n" + topology.describe() +
                "You can see it in http://zz85.github.io/kafka-streams-viz\n\n" +
                "Alternatively you can run ~/Downloads/apache-tomcat-9.0.39/bin/catalina.sh start\n" +
                "and use your local url http://localhost:8080/kafka-streams-viz/\n" +
                "If you want to play around, save the png graph topology obtained and open it in Chrome url " +
                "https://cloudapps.herokuapp.com/imagetoascii/" +
                "\n||||||||||||||||||\n");

        testDriver = new TopologyTestDriver(topology,
                               O04_sessionWindow.getStreamsConfiguration("localhost:9092"));

        testInputTopic =
                testDriver.createInputTopic(O04_sessionWindow.inputTopic,
                                            stringSerializer, avroSerde.serializer());
        testCountTableOutputTopic =
                testDriver.createOutputTopic(O04_sessionWindow.countTableOutputTopic,
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

        List<TestRecord<String, GenericRecord>> inputValues1 = Arrays.asList(
                    // key="site1", session window starting at 0
                    new TestRecord<>("site1",
                             createGenericRecord("site1", String.valueOf(random.nextInt(1000)), String.valueOf(System.currentTimeMillis())),
                             ZonedDateTime.of(2020, 3, 30, 0, 0, 20, 0, zone).toInstant()
                    ),
                    // key="site2", session window starting at 3
                    new TestRecord<>("site2",
                            createGenericRecord("site2", String.valueOf(random.nextInt(1000)), String.valueOf(System.currentTimeMillis())),
                            ZonedDateTime.of(2020, 3, 30, 0, 3, 14, 0, zone).toInstant()
                    ),
                    // key="site1", new session window because inactivity of this key, starting at 7
                    new TestRecord<>("site1",
                            createGenericRecord("site1", String.valueOf(random.nextInt(1000)), String.valueOf(System.currentTimeMillis())),
                            ZonedDateTime.of(2020, 3, 30, 0, 7, 10, 0, zone).toInstant()
                    ),
                    // key="site2", new session window because inactivity of this key, starting at 9
                    new TestRecord<>("site2",
                            createGenericRecord("site2", String.valueOf(random.nextInt(1000)), String.valueOf(System.currentTimeMillis())),
                            ZonedDateTime.of(2020, 3, 30, 0, 9, 30, 0, zone).toInstant()
                    ),
                    flushInputValuesToTopic()
                );

        List<TestRecord<String, GenericRecord>> inputValues2 = Arrays.asList(
                // key="site1", session window starting at 0
                new TestRecord<>("site3",
                        createGenericRecord("site1", String.valueOf(random.nextInt(1000)), String.valueOf(System.currentTimeMillis())),
                        ZonedDateTime.of(2020, 3, 30, 0, 0, 22, 0, zone).toInstant()
                ),
                // key="site2", session window starting at 3
                new TestRecord<>("site4",
                        createGenericRecord("site2", String.valueOf(random.nextInt(1000)), String.valueOf(System.currentTimeMillis())),
                        ZonedDateTime.of(2020, 3, 30, 0, 3, 24, 0, zone).toInstant()
                ),
                // key="site1", new session window because inactivity of this key, starting at 7
                new TestRecord<>("site3",
                        createGenericRecord("site1", String.valueOf(random.nextInt(1000)), String.valueOf(System.currentTimeMillis())),
                        ZonedDateTime.of(2020, 3, 30, 0, 4, 30, 0, zone).toInstant()
                ),
                // key="site2", new session window because inactivity of this key, starting at 9
                new TestRecord<>("site4",
                        createGenericRecord("site2", String.valueOf(random.nextInt(1000)), String.valueOf(System.currentTimeMillis())),
                        ZonedDateTime.of(2020, 3, 30, 0, 6, 39, 0, zone).toInstant()
                ),
                // key="site1", new session window because inactivity of this key, starting at 7
                new TestRecord<>("site3",
                        createGenericRecord("site1", String.valueOf(random.nextInt(1000)), String.valueOf(System.currentTimeMillis())),
                        ZonedDateTime.of(2020, 3, 30, 0, 8, 44, 0, zone).toInstant()
                ),
                flushInputValuesToTopic()
        );

        Map<String, String> expectedOutputMappedValues = new HashMap<>();

        // Records of a key arriving late to a session window (more than 5 minutes latency), are included
        // in a new session window
        // Session window 1 for key="site1", starting at 0
        expectedOutputMappedValues.put("timestamp: 2020-03-30T00:00:20Z, count: 1", "site1");
        // Session window 1 for key="site2", starting at 3
        expectedOutputMappedValues.put("timestamp: 2020-03-30T00:03:14Z, count: 1", "site2");
        // Session window 2 for key="site1", starting at 7. A new session window was created for this key
        // because of the inactivity exceeded 5 minutes for this key
        expectedOutputMappedValues.put("timestamp: 2020-03-30T00:07:10Z, count: 1", "site1");
        // Session window 2 for key="site2", starting at 9. A new session window was created for this key
        // because of hte inactivity exceeded 5 minutes for this key
        expectedOutputMappedValues.put("timestamp: 2020-03-30T00:09:30Z, count: 1", "site2");

        // Records of a key arriving on time to a session window (less than 5 minutes latency), are included
        // in the existing session window for that particular key

        // Session window 1 for key="site3", starting at 0
        expectedOutputMappedValues.put("timestamp: 2020-03-30T00:00:22Z, count: 1", "site3");
        // A new record with key="site3" arrives on time to Session window 1 for that partiular key:
        // The session wiwndow record count value is increased by 1
        expectedOutputMappedValues.put("timestamp: 2020-03-30T00:00:22Z, count: null", "site3");
        expectedOutputMappedValues.put("timestamp: 2020-03-30T00:04:30Z, count: 2", "site3");
        // A new record with key="site3" arrives on time to Session window 1 for that partiular key:
        // The session wiwndow record count value is increased by 1
        expectedOutputMappedValues.put("timestamp: 2020-03-30T00:04:30Z, count: null", "site3");
        expectedOutputMappedValues.put("timestamp: 2020-03-30T00:08:44Z, count: 3", "site3");


        // Session window 1 for key="site4", starting at 3
        expectedOutputMappedValues.put("timestamp: 2020-03-30T00:03:24Z, count: 1", "site4");
        // A new record with key="site3" arrives on time to Session window 1 for that partiular key:
        // The session wiwndow record count value is increased by 1
        expectedOutputMappedValues.put("timestamp: 2020-03-30T00:03:24Z, count: null", "site4");
        expectedOutputMappedValues.put("timestamp: 2020-03-30T00:06:39Z, count: 2", "site4");

        testInputTopic.pipeRecordList(inputValues1);
        testInputTopic.pipeRecordList(inputValues2);

        theTopologyMustProduce(expectedOutputMappedValues);
    }

    private void theTopologyMustProduce(Map<String, String> expectedOutputMappedValues) {
        testCountTableOutputTopic.readRecordsToList().forEach(kv -> {
                String composedKey = "timestamp: "+kv.getRecordTime()+", count: "+kv.getValue();
                String expectedValue = kv.getKey().substring(0, 5);
                    System.out.println("key="+kv.getKey().substring(0, 5)+
                            ", value="+kv.getValue()+", recordtime="+kv.getRecordTime());
//                    System.out.println("expectedOutputMappedValues for this key: "+expectedOutputMappedValues.get(composedKey));
                assertThat(expectedOutputMappedValues.get(composedKey), IsEqual.equalTo(expectedValue));
            }
        );

        // After consuming all the records from the output topic, no more records must exist in the output in topic
        assertTrue(testCountTableOutputTopic.isEmpty());
    }

    public static GenericRecord createGenericRecord(String siteId, String visitorId, String timestampMs) {
        EventMock eventMock = new EventMock();
        eventMock.site_id = siteId;
        eventMock.visitor = visitorId;
        eventMock.timestamp_ms = timestampMs;

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
        String timestamp_ms;
        String site_id;
        String visitor;
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
