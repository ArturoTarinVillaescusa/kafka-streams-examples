package io.confluent.examples.streams.streamdsl.stateless;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Stream processing unit test of {@link O4_branch}, using TopologyTestDriver.
 *
 * See {@link O4_branch} for further documentation.
 */
public class O4_branchTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> testInputTopic;
    private TestOutputTopic<String, String> testOutputTopic0to9;
    private TestOutputTopic<String, String> testOutputTopic10to99;
    private TestOutputTopic<String, String> testOutputTopicRestOfTheKeys;

    private StringSerializer stringSerializer = new StringSerializer();
    private StringDeserializer stringDeserializer = new StringDeserializer();

    @Before
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();

        // Create actual StreamBuilder topology
        O4_branch.createStream(builder);

        Topology topology = builder.build();

        System.out.println("\n||||||||||||||||||\n\n" + topology.describe() +
                "You can see it in http://zz85.github.io/kafka-streams-viz\n\n" +
                "Alternatively you can run ~/Downloads/apache-tomcat-9.0.39/bin/catalina.sh start\n" +
                "and use your local url http://localhost:8080/kafka-streams-viz/\n" +
                "If you want to play around, save the png graph topology obtained and open it in Chrome url " +
                "https://cloudapps.herokuapp.com/imagetoascii/" +
                "\n||||||||||||||||||\n");

        testDriver = new TopologyTestDriver(topology,
                O4_branch.getStreamsConfiguration("localhost:9092"));

        testInputTopic =
                testDriver.createInputTopic(O4_branch.inputTopic,
                                            stringSerializer, stringSerializer);
        testOutputTopic0to9 =
                testDriver.createOutputTopic(O4_branch.outputTopic0to9,
                                             stringDeserializer,stringDeserializer);
        testOutputTopic10to99 =
                testDriver.createOutputTopic(O4_branch.outputTopic10to99,
                        stringDeserializer,stringDeserializer);

        testOutputTopicRestOfTheKeys =
                testDriver.createOutputTopic(O4_branch.outputTopicRestOfTheKeys,
                        stringDeserializer,stringDeserializer);

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
     *  Simple test validating branch stateless transformation
     */
    @Test
    public void sourceStreamRecordSplittedInThreeBranches() {
        List<KeyValue<String, String>> inputValues =
                Arrays.asList(  new KeyValue<>("1","value01"),
                                new KeyValue<>("2","value02"),
                                new KeyValue<>("3","value03"),
                                new KeyValue<>("4","value04"),
                                new KeyValue<>("5","value05"),
                                new KeyValue<>("6","value06"),
                                new KeyValue<>("7","value07"),
                                new KeyValue<>("8","value08"),
                                new KeyValue<>("9","value09"),
                                new KeyValue<>("10","value10"),
                                new KeyValue<>("11","value11"),
                                new KeyValue<>("12","value12"),
                                new KeyValue<>("13","value13"),
                                new KeyValue<>("14","value14"),
                                new KeyValue<>("15","value15"),
                                new KeyValue<>("16","value16"),
                                new KeyValue<>("17","value17"),
                                new KeyValue<>("18","value18"),
                                new KeyValue<>("19","value19"),
                                new KeyValue<>("20","value20"),
                                new KeyValue<>("21","value21"),
                                new KeyValue<>("22","value22"),
                                new KeyValue<>("100","value100"),
                                new KeyValue<>("101","value101"),
                                new KeyValue<>("102","value102"),
                                new KeyValue<>("103","value103")
                );
        Map<String, String> outputValuesFrom0to9 = new HashMap<>();
        outputValuesFrom0to9.put("1","value01");
        outputValuesFrom0to9.put("2","value02");
        outputValuesFrom0to9.put("3","value03");
        outputValuesFrom0to9.put("4","value04");
        outputValuesFrom0to9.put("5","value05");
        outputValuesFrom0to9.put("6","value06");
        outputValuesFrom0to9.put("7","value07");
        outputValuesFrom0to9.put("8","value08");
        outputValuesFrom0to9.put("9","value09");

        Map<String, String> outputValuesFrom10to99 = new HashMap<>();
        outputValuesFrom10to99.put("10","value10");
        outputValuesFrom10to99.put("11","value11");
        outputValuesFrom10to99.put("12","value12");
        outputValuesFrom10to99.put("13","value13");
        outputValuesFrom10to99.put("14","value14");
        outputValuesFrom10to99.put("15","value15");
        outputValuesFrom10to99.put("16","value16");
        outputValuesFrom10to99.put("17","value17");
        outputValuesFrom10to99.put("18","value18");
        outputValuesFrom10to99.put("19","value19");
        outputValuesFrom10to99.put("20","value20");
        outputValuesFrom10to99.put("21","value21");
        outputValuesFrom10to99.put("22","value22");

        Map<String, String> outputTopicRestOfTheKeys = new HashMap<>();
        outputTopicRestOfTheKeys.put("100","value100");
        outputTopicRestOfTheKeys.put("101","value101");
        outputTopicRestOfTheKeys.put("102","value102");
        outputTopicRestOfTheKeys.put("103","value103");

        testInputTopic.pipeKeyValueList(inputValues,
                Instant.ofEpochMilli(0), Duration.ofMillis(100));
        assertThat(testOutputTopic0to9.readKeyValuesToMap(), equalTo(outputValuesFrom0to9));
        //No more output in topic
        assertTrue(testOutputTopic0to9.isEmpty());

        assertThat(testOutputTopic10to99.readKeyValuesToMap(), equalTo(outputValuesFrom10to99));
        //No more output in topic
        assertTrue(testOutputTopic10to99.isEmpty());

        assertThat(testOutputTopicRestOfTheKeys.readKeyValuesToMap(), equalTo(outputTopicRestOfTheKeys));
        //No more output in topic
        assertTrue(testOutputTopicRestOfTheKeys.isEmpty());
    }

}
