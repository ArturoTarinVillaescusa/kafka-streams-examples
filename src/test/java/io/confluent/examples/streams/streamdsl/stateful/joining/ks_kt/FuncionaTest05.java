package io.confluent.examples.streams.streamdsl.stateful.joining.ks_kt;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.confluent.examples.streams.IntegrationTestUtils.mkEntry;
import static io.confluent.examples.streams.IntegrationTestUtils.mkMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class FuncionaTest05 {

  private static final String leftTopic = "input-stream";
  private static final String rightTopic = "input-table";
  private static final String outputTopic = "innerjoin-stream-output";
  private TestInputTopic<String, String> testInputTopic1;
  private TestInputTopic<String, String> testInputTopic2;
  private TestOutputTopic<String, String> testInnerJoinOutputTopic;

  @Test
  public void shouldWorkAsExpected() {
    System.out.println("Seee the https://www.confluent.io/blog/distributed-real-time-joins-and-aggregations-on-user-activity-events-using-kafka-streams/");

    final List<KeyValue<String, String>> leftStreamInputValues = Arrays.asList(
      new KeyValue<>("1", "ValueFromEventStream")
    );

    final List<KeyValue<String, String>> rightTableInputValues = Arrays.asList(
      new KeyValue<>("1", "ValueFromEnrichTable")
    );

    final Map<String, String> expectedOutput = mkMap(
      mkEntry("1", "Left=ValueFromEventStream, Right=ValueFromEnrichTable")
    );

    final StreamsBuilder builder = new StreamsBuilder();

    O01_innerJoin.createStream(builder);


    Topology topology = builder.build();

    System.out.println("\n||||||||||||||||||\n" + topology.describe() +
            "You can see it in http://localhost:8080/kafka-streams-viz/\n" +
            "For that you must run ~/Downloads/apache-tomcat-9.0.39/bin/catalina.sh start" +
            "\n||||||||||||||||||\n");

    final TopologyTestDriver topologyTestDriver =
                 new TopologyTestDriver(topology,
                                        O01_innerJoin.getStreamsConfiguration("localhost:9092"));
    testInputTopic1 = topologyTestDriver
      .createInputTopic(leftTopic,
                        new StringSerializer(),
                        new StringSerializer());
    testInputTopic2 = topologyTestDriver
      .createInputTopic(rightTopic,
                        new StringSerializer(),
                        new StringSerializer());
    testInnerJoinOutputTopic = topologyTestDriver
      .createOutputTopic(outputTopic,
                         new StringDeserializer(),
                         new StringDeserializer());

    // In order to trigger the join, first, the table must have matching elements
    testInputTopic2.pipeKeyValueList(rightTableInputValues);
    // After the table has elements, records arriving to the stream will start triggering joins with
    // the matching elements in the table
    testInputTopic1.pipeKeyValueList(leftStreamInputValues);


    assertThat(testInnerJoinOutputTopic.readKeyValuesToMap(), equalTo(expectedOutput));
  }

}
