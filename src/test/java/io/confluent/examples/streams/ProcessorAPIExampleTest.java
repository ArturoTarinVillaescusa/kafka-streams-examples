/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.examples.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.confluent.examples.streams.IntegrationTestUtils.mkEntry;
import static io.confluent.examples.streams.IntegrationTestUtils.mkMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 In Kafka Streams, there are two ways you can specify your application logic—via the Processor API or the Streams DSL.
 Both of them will construct the application’s computational logic as a processor topology, which is represented as a
 graph of stream processor nodes that are connected by stream edges.

 Let’s take a look at a simple Streams application that performs the following steps:

 Consumes from a source topic named input
 Filters based on the contents of the value
 Transforms the value by extracting the first three characters
 Writes the updated key/value pairs to a topic named output

 Using the Processor API, you have full control constructing the topology graph by adding processor nodes and connecting
 them together. Note that it is also possible to add state stores to the topology and connect them to processor nodes,
 though such functionality is omitted in this example.

 When building a topology with the Processor API, you explicitly name each processing node in the topology, and also
 provide the name(s) of all of its parent nodes (the only exception are source nodes, which do not have any parents).
 For example, when adding the processor node named MappingProcessor, we declare its parent node is FilteringProcessor.
 Note that the MappingProcessor and FilteringProcessor code is omitted here for clarity.

 Below shows how this simple application can be written with the Processor API:
 */
public class ProcessorAPIExampleTest {

  private static final String inputTopic = "input-topic";
  private static final String substrOutputTopic = "substr-output-topic";
  private static final String upperOutputTopic = "upper-output-topic";

  @Test
  public void shouldBuildUseAndConsumeA101Graph() {
    System.out.println("https://www.confluent.io/blog/optimizing-kafka-streams-applications/");

    // Records to be published in the input topic
    final List<KeyValue<String, String>> inputRecords = Arrays.asList(
            new KeyValue<>("alice", "asia"),   /* Alice lived in Asia originally... */
            new KeyValue<>("bob", "americasFOO"),
            new KeyValue<>("chao", "asia"),
            new KeyValue<>("dave", "europe"),
            new KeyValue<>("alice", "europeFOO"),
            new KeyValue<>("eve", "americas"),
            new KeyValue<>("fang", "asia")
    );

    // Records expected to arrive to the substr-output-topic topic after applying the 1st branch Streams Topology transformations
    final Map<String, String> expectedMappedSubstrOutputRecords = mkMap(
            mkEntry("bob", "ame"),
            mkEntry("alice", "eur")
    );

    // Records expected to arrive to the upper-output-topic topic after applying the 2nd branch Streams Topology transformations
    final Map<String, String> expectedMappedUpperOutputRecords = mkMap(
            mkEntry("bob", "A"),
            mkEntry("alice", "E")
    );

    //
    // Step 1: Configure and start the processor topology.
    //
    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-kafka-101-api-graph-topology");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config");
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    // Use a temporary directory for storing state, which will be automatically removed after the test.
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

    // Create a state store manually.
    final StoreBuilder<KeyValueStore<String, String>> inputTopicStateStore = Stores
            .keyValueStoreBuilder(
                    Stores.persistentKeyValueStore("InputTopicStateStore"),
                    Serdes.String(),
                    Serdes.String())
            .withCachingEnabled();

    final Topology topology = new Topology();

    topology.addSource("SourceTopicProcessor",inputTopic );
    topology.addProcessor("FilteringProcessor", FilterProcessor::new, "SourceTopicProcessor");
    topology.addProcessor("MappingSubstrProcessor", MapSubstrProcessor::new, "FilteringProcessor");
    topology.addProcessor("MapUpperProcessor", MapUpperProcessor::new, "MappingSubstrProcessor");
    topology.addStateStore(inputTopicStateStore, "MapUpperProcessor");
    topology.addSink("SubstrSinkProcessor", substrOutputTopic, "MappingSubstrProcessor");
    topology.addSink("UpperSinkProcessor", upperOutputTopic, "MapUpperProcessor");

    System.out.println("\n||||||||||||||||||\n" + topology.describe() +
            "You can see it in http://localhost:8080/kafka-streams-viz/\n" +
            "For that you must run ~/Downloads/apache-tomcat-9.0.39/bin/catalina.sh start\n" +
            "You can also open the resulting 'png' image  in Chrome url https://cloudapps.herokuapp.com/imagetoascii/" +
            "\n||||||||||||||||||\n");


    try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, streamsConfiguration)) {
      //
      // Step 2: Setup input and output topics.
      //
      final TestInputTopic<String, String> inputTopic = topologyTestDriver
              .createInputTopic(ProcessorAPIExampleTest.inputTopic,
                      new StringSerializer(),
                      new StringSerializer());

      final TestOutputTopic<String, String> branchSubstrOutput = topologyTestDriver
              .createOutputTopic(substrOutputTopic,
                      new StringDeserializer(),
                      new StringDeserializer());

      final TestOutputTopic<String, String> branchUpperOutput = topologyTestDriver
              .createOutputTopic(upperOutputTopic,
                      new StringDeserializer(),
                      new StringDeserializer());

      //
      // Step 3: Publish messages to the input topic
      inputTopic.pipeKeyValueList(inputRecords);

      //
      // Step 4: Verify the output data.
      //
      assertThat(branchSubstrOutput.readKeyValuesToMap(), equalTo(expectedMappedSubstrOutputRecords));
      assertThat(branchUpperOutput.readKeyValuesToMap(), equalTo(expectedMappedUpperOutputRecords));

    }

  }

  static class FilterProcessor extends AbstractProcessor<String, String> {
    @Override
    public void process(String key, String value) {
      if (value.endsWith("FOO")) {
        context().forward(key, value);
      }
    }
  }

  static class MapSubstrProcessor extends AbstractProcessor<String, String> {

    @Override
    public void process(String key, String value) {
      context().forward(key, value.substring(0,3));
      // Processor API gives flexibility to forward KV pairs
      // to arbitrary child nodes
      context().forward(key, value.substring(0,1), To.child("MapUpperProcessor"));
    }
  }

  static class MapUpperProcessor extends AbstractProcessor<String, String> {
    @Override
    public void process(String key, String value) {
      /*
      org.apache.kafka.streams.errors.StreamsException: Processor MapUpperProcessor
      has no access to StateStore INPUTTOPICSTATESTORE as the store is not connected to the processor.
       If you add stores manually via '.addStateStore()' make sure to connect the added store to the
        processor by providing the processor name to '.addStateStore()' or connect them via
        '.connectProcessorAndStateStores()'. DSL users need to provide the store name to '.process()',
         '.transform()', or '.transformValues()' to connect the store to the corresponding operator,
          or they can provide a StoreBuilder by implementing the stores() method on the Supplier itself.
       */
//      context().getStateStore("InputTopicStateStore".toUpperCase());
      context().forward(key, value.toUpperCase());
    }
  }
}
