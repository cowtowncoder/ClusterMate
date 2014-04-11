package com.fasterxml.clustermate.service.metrics;

import com.codahale.metrics.*;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

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
	private final static double NANOS_TO_MILLIS_MULTIPLIER = 1.0 / (1000.0 * 1000.0);
	
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

    /**
     * For requests that can contain multiple entries (like SYNC-PULL),
     * distribution of per-request counts are included here.
     */
    public Histogram requestEntryCounts;

    /**
     * Optional extra information about queuing; currently only used with
     * DELETE operations.
     */
    public DeferQueueMetrics queue;

    protected ExternalOperationMetrics(OperationMetrics raw)
    {
        inFlight = raw._metricInFlight.getCount();

        count = raw._metricTimes.getCount();
        
        rate1Min = _doubleToInt(raw._metricTimes.getOneMinuteRate());
        rate5Min = _doubleToInt(raw._metricTimes.getFiveMinuteRate());
        rate15Min = _doubleToInt(raw._metricTimes.getFifteenMinuteRate());
        rateMean = _doubleToInt(raw._metricTimes.getMeanRate());

        requestTimes = _histogram(raw._metricTimes, true);
        requestSizes = _histogram(raw._metricSizes, false);
        requestEntryCounts = _histogram(raw._metricEntryCounts, false);
    }
    
    public static ExternalOperationMetrics create(OperationMetrics raw) {
        if (raw == null) {
            return null;
        }
        return new ExternalOperationMetrics(raw);
    }
    
    private static Histogram _histogram(Sampling src, boolean nanosToMillis)
    {
        if (src == null) {
            return null;
        }
        return new Histogram(src.getSnapshot(), nanosToMillis);
    }

    private static int _doubleToInt(double d) {
        return (int) (d + 0.5);
    }
    
    private static int _nanosToMillis(double rawTimeNanosecs) {
        // Since 3.0, times are in nanoseconds; used to be in milliseconds.
        return (int) Math.round(rawTimeNanosecs * NANOS_TO_MILLIS_MULTIPLIER);
    }
    
    public static class Histogram
    {
        public int pct50;
        public int pct75;
        public int pct95;
        public int pct99;
        public int pct999;

        protected Histogram() { } // if deserializing
        public Histogram(Snapshot snap, boolean nanosToMillis)
        {
        	if (nanosToMillis) {
	            pct50 = _nanosToMillis(snap.getMedian());
	            pct75 = _nanosToMillis(snap.get75thPercentile());
	            pct95 = _nanosToMillis(snap.get95thPercentile());
	            pct99 = _nanosToMillis(snap.get99thPercentile());
	            pct999 = _nanosToMillis(snap.get999thPercentile());
        	} else {
	            pct50 = _doubleToInt(snap.getMedian());
	            pct75 = _doubleToInt(snap.get75thPercentile());
	            pct95 = _doubleToInt(snap.get95thPercentile());
	            pct99 = _doubleToInt(snap.get99thPercentile());
	            pct999 = _doubleToInt(snap.get999thPercentile());
        	}
        }
    }
}
