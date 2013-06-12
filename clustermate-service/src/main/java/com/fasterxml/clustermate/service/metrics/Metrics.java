package com.fasterxml.clustermate.service.metrics;

import java.util.HashMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

/**
 * Simple container for Metrics 3.0 registry.
 * Bit complicated to work around issues with unit tests, where metrics are registered
 * multiple types.
 *<p>
 * May need to consider making injectable (or passed via contexts).
 */
public class Metrics
{
    protected final static MetricRegistry _metrics = new MetricRegistry();

    protected final static HashMap<String, Metric> _registered = new HashMap<String,Metric>();
    
    public static synchronized Counter newCounter(String name) {
        Counter c = (Counter) _registered.get(name);
        if (c == null) {
            c = _metrics.counter(name);
            _registered.put(name,  c);
        }
        return c;
    }

    public static synchronized Timer newTimer(String name)
    {
        Timer t = (Timer) _registered.get(name);
        if (t == null) {
            t = _metrics.timer(name);
            _registered.put(name, t);
        }
        return t;
    }

    public static synchronized Histogram newHistogram(String name)
    {
        Histogram h = (Histogram) _registered.get(name);
        if (h == null) {
            // default would be ExpDecayingReservoir; but let's make explicit
            h = _metrics.register(name, new Histogram(new ExponentiallyDecayingReservoir()));
            _registered.put(name,  h);
        }
        return h;
    }
}
