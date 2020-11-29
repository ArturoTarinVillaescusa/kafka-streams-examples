package io.confluent.examples.streams.streamdsl.interactivequeries.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

import static io.confluent.examples.streams.streamdsl.processorapi.process.O01_processStrategy1_ImplementProcessor.stateStoreName;

@Slf4j
public class EmailMetricThresholdAlert_Processor implements Processor<String, String> {
    private KeyValueStore<String, String> stateStore;
    private ProcessorContext context = null;

    public EmailMetricThresholdAlert_Processor() {

    }

    @Override
    public void init(ProcessorContext processorContext) {

        this.context = processorContext;
        this.stateStore = (KeyValueStore) processorContext.getStateStore(stateStoreName);

        // Here you would perform any additional initializations such as setting up an email client.

        StateRestoreCallback stateRestoreCallback = new StateRestoreCallback() {
            @Override
            public void restore(byte[] bytes, byte[] bytes1) {
                // TODO: MUST DO SOMETHING TO RESTORE IT
                log.info("Restoring bytes={}, bytes1={}", bytes, bytes1);
            }
        };
        // Would fail because State Store has already been registered
//        this.context.register(this.stateStore, stateRestoreCallback);

        // Punctuate each second, can access this.stateStore
        //  // processes all the messages in the state store and sends single aggregate message
        Punctuator punctuator = new Punctuator() {
            @Override
            public void punctuate(long l) {
                KeyValueIterator<String, String> all = stateStore.all();
                log.info("................... Punctuate method, l = {}", l);
            }
        };
        processorContext.schedule(Duration.ofMillis(1),
                                  PunctuationType.WALL_CLOCK_TIME, punctuator);
    }


    @Override
    public void process(String key, String value) {

        // Here you would format and send the alert email.
        //
        // In this specific example, you would be able to include information about the metricID
        // and its value (because the class implements `Processor<String, String>`).

        // For example, we can skip storing keys or values that meet some condition,
        // but it is better if we use the .filter( or .filterNot DSL in the STream
        if (!key.equals("FOO")) {
            String oldStore = stateStore.get(key);
            String changedValue = value.toUpperCase();
            stateStore.put(key, changedValue);
            KeyValueIterator<String, String> allStateStoreValues = stateStore.all();
            allStateStoreValues.forEachRemaining(c -> {
                log.info("SEND EMAIL WITH >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> {}", c.toString());
            });

            context.forward(key,changedValue);
        }
    }

    @Override
    public void close() {
        // can access this.state
        stateStore.close();
    }

}
