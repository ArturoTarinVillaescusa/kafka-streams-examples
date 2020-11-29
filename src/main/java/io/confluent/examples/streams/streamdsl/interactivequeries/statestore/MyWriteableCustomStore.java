package io.confluent.examples.streams.streamdsl.interactivequeries.statestore;

// Read-write interface for MyCustomStore
public interface MyWriteableCustomStore<K, V> extends MyReadableCustomStore<K, V> {
    void write(K key, V value);
}
