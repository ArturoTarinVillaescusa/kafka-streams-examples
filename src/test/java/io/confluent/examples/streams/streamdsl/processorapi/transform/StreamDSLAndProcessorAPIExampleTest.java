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
package io.confluent.examples.streams.streamdsl.processorapi.transform;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
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

 Below shows how a simple application can combine Processor API and Streams DSL, giving you the best of both worlds:
 starting with a Streams DSL  object and then involving a transformValues operator.

 This operator can take an arbitrary transform processor similar to the Processor API and be associated with a
 state store named stateStore to be accessed within the processor.
 */
public class StreamDSLAndProcessorAPIExampleTest {

  private static final String inputTopicName = "input-topic";
  private static final String substrOutputTopicName = "substr-output-topic";
  private static final String upperOutputTopicName = "upper-output-topic";
  private static final String storeName = "state-store";

  @Test
  public void shouldBuildUseAndConsumeA101Graph() {
    System.out.println("https://www.confluent.io/blog/optimizing-kafka-streams-applications/");

    // Records to be published in the input topic
    final List<KeyValue<String, String>> inputRecords = Arrays.asList(
            new KeyValue<>("alice", "asiaFOO"),
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
            mkEntry("bob", "americasFOO_SUPERCLOSURE"),
            mkEntry("alice", "europeFOO_SUPERCLOSURE")
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

    final StreamsBuilder builder = new StreamsBuilder();

    final StoreBuilder<KeyValueStore<String, String>> storeBuilder =
            Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(storeName),
                                        Serdes.String(),
                                        Serdes.String());

    builder.addStateStore(storeBuilder);

    builder.<String, String>stream(inputTopicName)
            .filter((k, v) -> v.endsWith("FOO"))
            .transformValues(() -> new SimpleValueTransformer(storeName), storeName)
            .to(upperOutputTopicName);

    final Topology topology = builder.build(streamsConfiguration);

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
              .createInputTopic(inputTopicName,
                      new StringSerializer(),
                      new StringSerializer());

      final TestOutputTopic<String, String> branchUpperOutput = topologyTestDriver
              .createOutputTopic(upperOutputTopicName,
                      new StringDeserializer(),
                      new StringDeserializer());

      //
      // Step 3: Publish messages to the input topic
      inputTopic.pipeKeyValueList(inputRecords);

      //
      // Step 4: Verify the output data.
      //
      assertThat(branchUpperOutput.readKeyValuesToMap(), equalTo(expectedMappedUpperOutputRecords));

    }

  }

  private class SimpleValueTransformer implements ValueTransformerWithKey<String, String, String> {
    private String storeName;
    private KeyValueStore<String, String> store;

    public SimpleValueTransformer(String storeName) {
      this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext context) {
      this.store = (KeyValueStore<String, String>) context.getStateStore(storeName);
    }

    @Override
    public String transform(String key, String value) {
      String persistedValue = store.get(key);
      String updatedValue = value + "_" + "SUPERCLOSURE";

      if (persistedValue == null) {
        store.put(key, updatedValue);
      }
      return updatedValue;
    }

    @Override
    public void close() {

    }
  }
}
