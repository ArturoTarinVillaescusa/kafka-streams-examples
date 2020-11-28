package io.confluent.examples.streams.streamdsl.processorapi.utils.transformers;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

import static io.confluent.examples.streams.streamdsl.processorapi.transform.O01_transformStrategy1_ImplementTransformer.stateStoreName;

@Slf4j
public class EmailMetricThresholdAlert_ValueTransformer implements ValueTransformer {
    private ProcessorContext context;
    private KeyValueStore stateStore;

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        this.stateStore = (KeyValueStore) context.getStateStore(stateStoreName);
        // Punctuate each millisecond; can access this.stateStore
        context.schedule(Duration.ofMillis(1),
                PunctuationType.WALL_CLOCK_TIME,
                new Punctuator() {
                    @Override
                    public void punctuate(long l) {
                        log.info("PUNCTUATOR SAYS: ...... {}", stateStore.all());
                    }
                });
    }

    @Override
    public Integer transform(Object value) {
        // can access this.stateStore
        stateStore.put(value, value);
        stateStore.all().forEachRemaining(c -> {
            KeyValue<String, String> cast = (KeyValue<String, String>) c;
            log.info("THE STATE STORE CONTAINS THIS: key={}, value={}", cast.key, cast.value);
        });
        return value.hashCode();
    }

    @Override
    public void close() {
        stateStore.close();
    }
}
