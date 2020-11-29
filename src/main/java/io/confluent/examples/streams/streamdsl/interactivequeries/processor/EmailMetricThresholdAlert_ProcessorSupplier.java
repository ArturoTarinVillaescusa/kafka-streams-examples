package io.confluent.examples.streams.streamdsl.interactivequeries.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;
import java.util.Set;

import static io.confluent.examples.streams.streamdsl.processorapi.process.O02_processStrategy2_ImplementProcessorSupplier.stateStoreName;

@Slf4j
public class EmailMetricThresholdAlert_ProcessorSupplier implements ProcessorSupplier<String, String> {

    public EmailMetricThresholdAlert_ProcessorSupplier() {

    }

    @Override
    // Supply processor
    public Processor<String, String> get() {
        return new EmailMetricThresholdAlert_Processor();
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        StoreBuilder<KeyValueStore<String, String>> keyValueStoreStoreBuilder =
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStoreName),
                        Serdes.String(), Serdes.String());
        return Collections.singleton(keyValueStoreStoreBuilder);
    }

}
