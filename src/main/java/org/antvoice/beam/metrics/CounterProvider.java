package org.antvoice.beam.metrics;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

public class CounterProvider implements Serializable {

    private final ConcurrentHashMap<String, Counter> _counters = new ConcurrentHashMap<>();

    public Counter getCounter(String namespace, String name){
        return _counters.computeIfAbsent(name, s -> Metrics.counter(namespace, name));
    }

}
