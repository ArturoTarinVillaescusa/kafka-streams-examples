package io.confluent.examples.streams.streamdsl.interactivequeries.statestore;

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.internals.StateStoreProvider;

public class MyCustomStoreType<K, V> implements QueryableStoreType<MyReadableCustomStore<K, V>> {
    // Only accepts StateStores that are of type MyCustomStore
    @Override
    public boolean accepts(StateStore stateStore) {
        return stateStore instanceof MyCustomStore;
    }

    @Override
    public MyReadableCustomStore<K, V> create(final StateStoreProvider stateStoreProvider,
                                              final String stateStoreName) {
        return (MyReadableCustomStore<K, V>)
                new MyCustomStoreTypeWrapper(stateStoreProvider, stateStoreName, this);
    }
}
