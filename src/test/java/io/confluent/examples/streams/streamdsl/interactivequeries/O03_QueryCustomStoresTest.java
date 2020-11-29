package io.confluent.examples.streams.streamdsl.interactivequeries;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Stream processing unit test of {@link O3_QueryCustomStores}, using TopologyTestDriver.
 *
 * See {@link O3_QueryCustomStores} for further documentation.
 */
public class O03_QueryCustomStoresTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> testInputTopic;
    private TestOutputTopic<String, String> testCountTableOutputTopic;
    private StringSerializer stringSerializer = new StringSerializer();
    private StringDeserializer stringDeserializer = new StringDeserializer();
    private IntegerDeserializer intDeserializer = new IntegerDeserializer();

    @Before
    public void setup() {

        final StreamsBuilder builder = new StreamsBuilder();

        Topology topology = O3_QueryCustomStores.buildTheTopology();

        System.out.println("\n||||||||||||||||||\n\n" + topology.describe() +
                "You can see it in http://zz85.github.io/kafka-streams-viz\n\n" +
                "Alternatively you can run ~/Downloads/apache-tomcat-9.0.39/bin/catalina.sh start\n" +
                "and use your local url http://localhost:8080/kafka-streams-viz/\n" +
                "If you want to play around, save the png graph topology obtained and open it in Chrome url " +
                "https://cloudapps.herokuapp.com/imagetoascii/" +
                "\n||||||||||||||||||\n");

        testDriver = new TopologyTestDriver(topology,
                               O3_QueryCustomStores.getStreamsConfiguration("localhost:9092"));

        testInputTopic =
                testDriver.createInputTopic(O3_QueryCustomStores.inputTopic,
                                            stringSerializer, stringSerializer);

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
                    ,new KeyValue<>("FOO", "asdasdf")
                );

        List<KeyValue<String, String>> expectedOutputMappedValues = Arrays.asList(
                new KeyValue<>("site1", "ASDASDF")
        );

        testInputTopic.pipeKeyValueList(inputValues1);

    }

}
