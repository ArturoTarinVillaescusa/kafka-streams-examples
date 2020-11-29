package io.confluent.examples.streams.streamdsl.interactivequeries.statestore;

import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.internals.StateStoreProvider;

import java.util.List;
import java.util.Optional;

public class MyCustomStoreTypeWrapper implements MyReadableCustomStore<K, V> {
    private final QueryableStoreType<MyReadableCustomStore<K, V>> customStoreType;
    private final String stateStoreName;
    private final StateStoreProvider provider;

    public <K, V> MyCustomStoreTypeWrapper(StateStoreProvider provider,
                                           String stateStoreName,
                                           MyCustomStoreType customStoreType) {
        this.customStoreType = customStoreType;
        this.stateStoreName = stateStoreName;
        this.provider = provider;
    }

    // Implement a safe read method
    @Override
    public V read(final K key) {
        // Get all the stores with stateStoreName and customStoreType
        final List<MyReadableCustomStore<K, V>> stores =
                provider.stores(stateStoreName, customStoreType);
        // Try and find the value for the given key
        final Optional<MyReadableCustomStore<K, V>> value =
                stores.stream().filter(store -> store.read(key) != null).findFirst();
        return value.orElse(null);
    }
}
