package io.confluent.examples.streams.streamdsl.interactivequeries;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Stream processing unit test of {@link O1_QueryLocalKeyValueStores}, using TopologyTestDriver.
 *
 * See {@link O1_QueryLocalKeyValueStores} for further documentation.
 */
public class O1_QueryLocalKeyValueStoresTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> testInputTopic;
    private TestOutputTopic<String, Long> testOutputTopic;
    private StringSerializer stringSerializer = new StringSerializer();
    private StringDeserializer stringDeserializer = new StringDeserializer();
    private LongDeserializer longDeserializer = new LongDeserializer();

    private List<KeyValue<String, String>> inputValues = Arrays.asList(
            new KeyValue<>("key01","This is the text assigned to the key01")
            ,new KeyValue<>("key02","And this is the one assigned to the key02")
    );

    @Before
    public void setup() {
        StreamsBuilder builder = new StreamsBuilder();

        // Create actual StreamBuilder topology
        O1_QueryLocalKeyValueStores.createStream(builder);

        Topology topology = builder.build();

        System.out.println("\n||||||||||||||||||\n\n" + topology.describe() +
                "You can see it in http://zz85.github.io/kafka-streams-viz\n\n" +
                "Alternatively you can run ~/Downloads/apache-tomcat-9.0.39/bin/catalina.sh start\n" +
                "and use your local url http://localhost:8080/kafka-streams-viz/\n" +
                "If you want to play around, save the png graph topology obtained and open it in Chrome url " +
                "https://cloudapps.herokuapp.com/imagetoascii/" +
                "\n||||||||||||||||||\n");

        testDriver = new TopologyTestDriver(topology,
                   O1_QueryLocalKeyValueStores
                           .getStreamsConfiguration("localhost:9092"));

        testInputTopic =
            testDriver.createInputTopic(O1_QueryLocalKeyValueStores.inputTopic,
                                        stringSerializer, stringSerializer);
        testOutputTopic =
            testDriver.createOutputTopic(O1_QueryLocalKeyValueStores.outputTopic,
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
        List<KeyValue<String, Long>> expectedOutputValues = Arrays.asList(
             new KeyValue<>("this", 1L)
            ,new KeyValue<>("is", 1L)
            ,new KeyValue<>("the", 1L)
            ,new KeyValue<>("text", 1L)
            ,new KeyValue<>("assigned", 1L)
            ,new KeyValue<>("to", 1L)
            ,new KeyValue<>("the", 2L)
            ,new KeyValue<>("key01", 1L)
            ,new KeyValue<>("and", 1L)
            ,new KeyValue<>("this", 2L)
            ,new KeyValue<>("is", 2L)
            ,new KeyValue<>("the", 3L)
            ,new KeyValue<>("one", 1L)
            ,new KeyValue<>("assigned", 2L)
            ,new KeyValue<>("to", 2L)
            ,new KeyValue<>("the", 4L)
            ,new KeyValue<>("key02", 1L)

        );

        testInputTopic.pipeKeyValueList(inputValues);

        assertThat(testOutputTopic.readKeyValuesToList(), equalTo(expectedOutputValues));

        //No more output in topic
        assertTrue(testOutputTopic.isEmpty());
    }

    @Test
    public void materializedResultIsOK() {

    }

    @Test
    public void nonMaterializedResultIsOK() {

    }


}
