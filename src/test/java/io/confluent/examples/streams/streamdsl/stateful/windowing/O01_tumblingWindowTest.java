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
 * Stream processing unit test of {@link O01_tumblingWindow}, using TopologyTestDriver.
 *
 * See {@link O01_tumblingWindow} for further documentation.
 */
public class O01_tumblingWindowTest {

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
        O01_tumblingWindow.createStream(builder);

        Topology topology = builder.build();

        System.out.println("\n||||||||||||||||||\n\n" + topology.describe() +
                "You can see it in http://zz85.github.io/kafka-streams-viz\n\n" +
                "Alternatively you can run ~/Downloads/apache-tomcat-9.0.39/bin/catalina.sh start\n" +
                "and use your local url http://localhost:8080/kafka-streams-viz/\n" +
                "If you want to play around, save the png graph topology obtained and open it in Chrome url " +
                "https://cloudapps.herokuapp.com/imagetoascii/" +
                "\n||||||||||||||||||\n");

        testDriver = new TopologyTestDriver(topology,
                               O01_tumblingWindow.getStreamsConfiguration("localhost:9092"));

        testInputTopic =
                testDriver.createInputTopic(O01_tumblingWindow.inputTopic,
                                            stringSerializer, avroSerde.serializer());
        testCountTableOutputTopic =
                testDriver.createOutputTopic(O01_tumblingWindow.countTableOutputTopic,
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
                    // key="site1", inside 0 to 5 minutes tumbling window
                    new TestRecord<>("site1",
                             createGenericRecord("site1", String.valueOf(random.nextInt(1000)), String.valueOf(System.currentTimeMillis())),
                             ZonedDateTime.of(2020, 3, 30, 0, 0, 20, 0, zone).toInstant()
                    ),
                    // key="site1", inside 0 to 5 minutes tumbling window
                    new TestRecord<>("site1",
                            createGenericRecord("site1", String.valueOf(random.nextInt(1000)), String.valueOf(System.currentTimeMillis())),
                            ZonedDateTime.of(2020, 3, 30, 0, 2, 14, 0, zone).toInstant()
                    ),
                    // key="site1", inside 0 to 5 minutes tumbling window
                    new TestRecord<>("site1",
                            createGenericRecord("site1", String.valueOf(random.nextInt(1000)), String.valueOf(System.currentTimeMillis())),
                            ZonedDateTime.of(2020, 3, 30, 0, 3, 10, 0, zone).toInstant()
                    ),
                    // key="site1", inside 5 to 10 minutes tumbling window
                    new TestRecord<>("site1",
                            createGenericRecord("site1", String.valueOf(random.nextInt(1000)), String.valueOf(System.currentTimeMillis())),
                            ZonedDateTime.of(2020, 3, 30, 0, 5, 1, 0, zone).toInstant()
                    ),
                    // key="site1", inside 5 to 10 minutes tumbling window
                    new TestRecord<>("site1",
                            createGenericRecord("site1", String.valueOf(random.nextInt(1000)), String.valueOf(System.currentTimeMillis())),
                            ZonedDateTime.of(2020, 3, 30, 0, 6, 30, 0, zone).toInstant()
                    ),
                    flushInputValuesToTopic()
                );

        List<TestRecord<String, GenericRecord>> inputValues2 = Arrays.asList(
                // key="site2", inside 0 to 5 minutes tumbling window
                new TestRecord<>("site2",
                        createGenericRecord("site2", String.valueOf(random.nextInt(1000)), String.valueOf(System.currentTimeMillis())),
                        ZonedDateTime.of(2020, 3, 30, 0, 3, 20, 0, zone).toInstant()
                ),
                // key="site2", inside 0 to 5 minutes tumbling window
                new TestRecord<>("site2",
                        createGenericRecord("site2", String.valueOf(random.nextInt(1000)), String.valueOf(System.currentTimeMillis())),
                        ZonedDateTime.of(2020, 3, 30, 0, 4, 14, 0, zone).toInstant()
                ),
                // key="site2", inside 5 to 10 minutes tumbling window
                new TestRecord<>("site2",
                        createGenericRecord("site2", String.valueOf(random.nextInt(1000)), String.valueOf(System.currentTimeMillis())),
                        ZonedDateTime.of(2020, 3, 30, 0, 5, 50, 0, zone).toInstant()
                ),
                // key="site2", inside 10 to 15 minutes tumbling window
                new TestRecord<>("site2",
                        createGenericRecord("site2", String.valueOf(random.nextInt(1000)), String.valueOf(System.currentTimeMillis())),
                        ZonedDateTime.of(2020, 3, 30, 0, 11, 34, 0, zone).toInstant()
                ),
                flushInputValuesToTopic()
        );

        Map<String, String> expectedOutputMappedValues = new HashMap<>();
        // 0 to 5 minutes tumbling window aggregation count for input key == "site1"
        expectedOutputMappedValues.put("timestamp: 2020-03-30T00:00:20Z, count: 1", "site1");
        expectedOutputMappedValues.put("timestamp: 2020-03-30T00:02:14Z, count: 2", "site1");
        expectedOutputMappedValues.put("timestamp: 2020-03-30T00:03:10Z, count: 3", "site1");

        // 5 to 10 minutes tumbling window aggregation count for input key == "site2"
        expectedOutputMappedValues.put("timestamp: 2020-03-30T00:05:01Z, count: 1", "site1");
        expectedOutputMappedValues.put("timestamp: 2020-03-30T00:06:30Z, count: 2", "site1");

        // 0 to 5 minutes tumbling window aggregation count for input key == "site2"
        expectedOutputMappedValues.put("timestamp: 2020-03-30T00:03:20Z, count: 1", "site2");
        expectedOutputMappedValues.put("timestamp: 2020-03-30T00:04:14Z, count: 2", "site2");

        // 5 to 10 minutes tumbling window aggregation count for input key == "site2"
        expectedOutputMappedValues.put("timestamp: 2020-03-30T00:05:50Z, count: 1", "site2");

        // 10 to 15 minutes tumbling window aggregation count for input key == "site2"
        expectedOutputMappedValues.put("timestamp: 2020-03-30T00:11:34Z, count: 1", "site2");

        testInputTopic.pipeRecordList(inputValues1);
        testInputTopic.pipeRecordList(inputValues2);

        theTopologyMustProduce(expectedOutputMappedValues);
    }

    private void theTopologyMustProduce(Map<String, String> expectedOutputMappedValues) {
        testCountTableOutputTopic.readRecordsToList().forEach(kv -> {
                String composedKey = "timestamp: "+kv.getRecordTime()+", count: "+kv.getValue();
                String expectedValue = kv.getKey().substring(0, 5);
//                    System.out.println("key="+kv.getKey().substring(0, 5)+
//                            ", value="+kv.getValue()+", recordtime="+kv.getRecordTime());
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
