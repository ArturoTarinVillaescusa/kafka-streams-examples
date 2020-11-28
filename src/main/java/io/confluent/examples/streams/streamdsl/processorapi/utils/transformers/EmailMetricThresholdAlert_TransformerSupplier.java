package io.confluent.examples.streams.streamdsl.processorapi.utils.transformers;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;
import java.util.Set;

import static io.confluent.examples.streams.streamdsl.processorapi.transform.O01_transformStrategy1_ImplementTransformer.stateStoreName;

public class EmailMetricThresholdAlert_TransformerSupplier implements TransformerSupplier {
    /***************************
     *  Supply our transformer
     ***************************/
    @Override
    public Transformer get() {

        return new EmailMetricThresholdAlert_Transformer();
    }

    /***************************
     * Provide State Stores that will be added and connected to the associated transformer
     * The store name from the builder, stateStoreName, is used to access the store later via the
     * ProcessorContext
     **************************
     * @return*/
    @Override
    public Set<StoreBuilder<?>> stores() {
        StoreBuilder keyValueStoreBuilder =
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStoreName),
                        Serdes.Integer(), Serdes.String());
        return Collections.singleton(keyValueStoreBuilder);
    }
}
