package io.confluent.examples.streams.streamdsl.interactivequeries.statestore;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;

/*
 * Implementation of the actual Custom State Store
 */
public class MyCustomStore<K, V> implements StateStore, MyWriteableCustomStore {
    KeyValueStore<Object, Object> store;
    ProcessorContext processor;

    @Override
    public void write(Object key, Object value) {
        store.put(key, value);
    }

    @Override
    public Object read(Object key) {

        return store.get(key);
    }

    @Override
    public String name() {

        return store.name();
    }

    @Override
    public void init(ProcessorContext processorContext, StateStore stateStore) {
       store  = (KeyValueStore<Object, Object>) stateStore;
       processor = processorContext;
    }

    @Override
    public void flush() {
        store.flush();
    }

    @Override
    public void close() {
        store.close();
    }

    @Override
    public boolean persistent() {

        return true;
    }

    @Override
    public boolean isOpen() {

        return true;
    }
}
