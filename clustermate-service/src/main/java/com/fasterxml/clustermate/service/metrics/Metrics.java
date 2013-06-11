package com.fasterxml.clustermate.service.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

/**
 * Simple container for Metrics 3.0 registry.
 *<p>
 * May need to consider making injectable (or passed via contexts).
 */
public class Metrics
{
    protected final static MetricRegistry _metrics = new MetricRegistry();

    public static Counter newCounter(String name) {
        return _metrics.counter(name);
    }

    public static Timer newTimer(String name) {
        return _metrics.timer(name);
    }

    public static Histogram newHistogram(String name) {
        // default would be ExpDecayingReservoir; but let's make explicit
        return _metrics.register(name, new Histogram(new ExponentiallyDecayingReservoir()));
    }
}
