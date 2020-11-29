package io.confluent.examples.streams.streamdsl.interactivequeries.statestore;

import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Map;

/*
 * Implementation of the supplier for MyCustomStore
 */
public class MyCustomStoreBuilder implements StoreBuilder<MyCustomStore<K, V>> {
    public MyCustomStoreBuilder(String s) {
    }

    @Override
    public StoreBuilder<MyCustomStore<K, V>> withCachingEnabled() {
        return null;
    }

    @Override
    public StoreBuilder<MyCustomStore<K, V>> withCachingDisabled() {
        return null;
    }

    @Override
    public StoreBuilder<MyCustomStore<K, V>> withLoggingEnabled(Map<String, String> map) {
        return null;
    }

    @Override
    public StoreBuilder<MyCustomStore<K, V>> withLoggingDisabled() {
        return null;
    }

    @Override
    public MyCustomStore<K, V> build() {
        return null;
    }

    @Override
    public Map<String, String> logConfig() {
        return null;
    }

    @Override
    public boolean loggingEnabled() {
        return false;
    }

    @Override
    public String name() {
        return null;
    }
}
