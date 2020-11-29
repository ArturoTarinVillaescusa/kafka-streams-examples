package io.confluent.examples.streams.streamdsl.interactivequeries.statestore;

// Read-only interface for MyCustomStore
public interface MyReadableCustomStore<K, V> extends io.confluent.examples.streams.streamdsl.interactivequeries.statestore.V {
    V read(K key);
}
