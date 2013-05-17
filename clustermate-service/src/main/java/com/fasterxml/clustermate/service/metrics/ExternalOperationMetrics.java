package com.fasterxml.clustermate.service.metrics;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.yammer.metrics.core.*;
import com.yammer.metrics.stats.Snapshot;

/**
 * Container class for exposing {@link OperationMetrics} externally,
 * usually as JSON, to be displayed on dashboards etc.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(alphabetic=true,
   value={ "count", "inFlight", "requestTimes" }
)
public class ExternalOperationMetrics
{
    /**
     * Total count from rate metrics
     */
    public long count;

    /**
     * Number of operations active (in-flight)
     */
    public long inFlight;

    public int rate1Min, rate5Min, rate15Min, rateMean;

    public Histogram requestTimes;

    public Histogram requestSizes;

    public Histogram requestEntryCounts;

    protected ExternalOperationMetrics(OperationMetrics raw)
    {
        inFlight = raw._metricInFlight.count();

        count = raw._metricTimes.count();
        rate1Min = (int) raw._metricTimes.oneMinuteRate();
        rate5Min = (int) raw._metricTimes.fiveMinuteRate();
        rate15Min = (int) raw._metricTimes.fifteenMinuteRate();
        rateMean = (int) raw._metricTimes.meanRate();

        requestTimes = _histogram(raw._metricTimes);
        requestSizes = _histogram(raw._metricSizes);
        requestEntryCounts = _histogram(raw._metricEntryCounts);
    }
    
    public static ExternalOperationMetrics create(OperationMetrics raw) {
        if (raw == null) {
            return null;
        }
        return new ExternalOperationMetrics(raw);
    }

    private static Histogram _histogram(Sampling src)
    {
        if (src == null) {
            return null;
        }
        return new Histogram(src.getSnapshot());

    }

    public static class Histogram
    {
        public int pct50;
        public int pct75;
        public int pct95;
        public int pct99;
        public int pct999;

        protected Histogram() { } // if deserializing
        public Histogram(Snapshot snap)
        {
            pct50 = (int) snap.getMedian();
            pct75 = (int) snap.get75thPercentile();
            pct95 = (int) snap.get95thPercentile();
            pct99 = (int) snap.get99thPercentile();
            pct999 = (int) snap.get999thPercentile();
        }
    }
}
