package io.confluent.examples.streams.streamdsl.processorapi.transformvalueswithkey;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.hamcrest.core.IsEqual;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Stream processing unit test of {@link O01_transformValuesStrategy1_ImplementTransformerWithKey}, using TopologyTestDriver.
 *
 * See {@link O01_transformValuesStrategy1_ImplementTransformerWithKey} for further documentation.
 */
public class O01_transformValuesStrategy1ImplementTransformerWithKeyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> testInputTopic;
    private TestOutputTopic<String, Integer> testCountTableOutputTopic;
    private StringSerializer stringSerializer = new StringSerializer();
    private StringDeserializer stringDeserializer = new StringDeserializer();
    private IntegerDeserializer intDeserializer = new IntegerDeserializer();

    @Before
    public void setup() {

        final StreamsBuilder builder = new StreamsBuilder();

        // Create actual StreamBuilder topology
        O01_transformValuesStrategy1_ImplementTransformerWithKey.createStream(builder);

        Topology topology = builder.build();

        System.out.println("\n||||||||||||||||||\n\n" + topology.describe() +
                "You can see it in http://zz85.github.io/kafka-streams-viz\n\n" +
                "Alternatively you can run ~/Downloads/apache-tomcat-9.0.39/bin/catalina.sh start\n" +
                "and use your local url http://localhost:8080/kafka-streams-viz/\n" +
                "If you want to play around, save the png graph topology obtained and open it in Chrome url " +
                "https://cloudapps.herokuapp.com/imagetoascii/" +
                "\n||||||||||||||||||\n");

        testDriver = new TopologyTestDriver(topology,
                               O01_transformValuesStrategy1_ImplementTransformerWithKey.getStreamsConfiguration("localhost:9092"));

        testInputTopic =
                testDriver.createInputTopic(O01_transformValuesStrategy1_ImplementTransformerWithKey.inputTopic,
                                            stringSerializer, stringSerializer);
        testCountTableOutputTopic =
                testDriver.createOutputTopic(O01_transformValuesStrategy1_ImplementTransformerWithKey.outputTopic,
                        stringDeserializer, intDeserializer);
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

        List<KeyValue<String, String>> inputValues1 = Arrays.asList(
                     new KeyValue<>("site1", "asdasdf")
                    ,new KeyValue<>("FOO", "asd")
                );

        List<KeyValue<String, Integer>> expectedOutputMappedValues = Arrays.asList(
                 new KeyValue<>("site1", -718748250)
                ,new KeyValue<>("FOO", 96882)
        );

        testInputTopic.pipeKeyValueList(inputValues1);

        assertThat(testCountTableOutputTopic.readKeyValuesToList(), IsEqual.equalTo(expectedOutputMappedValues));

        // After consuming all the records from the output topic, no more records must exist in the output in topic
        assertTrue(testCountTableOutputTopic.isEmpty());
    }

}
