package com.fasterxml.clustermate.service.metrics;

import java.util.concurrent.TimeUnit;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import com.fasterxml.clustermate.service.OperationDiagnostics;
import com.fasterxml.clustermate.service.cfg.ServiceConfig;
import com.fasterxml.storemate.store.Storable;

public class OperationMetrics
{
    protected final ServiceConfig _serviceConfig;

    /*
    /**********************************************************************
    /* Actual metric aggregators
    /**********************************************************************
     */
    
    protected final Counter _metricInFlight;

    protected final Meter _metricRate;

    protected final Timer _metricTimes;

    // // Size metrics (optional)
    
    protected final Histogram _metricSizes;

    protected final Histogram _metricEntryCounts;

    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */

    private OperationMetrics(ServiceConfig serviceConfig, String operationName,
            boolean includeSizes, boolean includeEntryCounts)
    {
        _serviceConfig = serviceConfig;
        String metricGroup = serviceConfig.metricsJmxRoot;

        // and then create metrics
        
        // first: in-flight counter, "active" requests
        _metricInFlight = Metrics.newCounter(new MetricName(metricGroup, operationName, "active"));
        _metricRate = Metrics.newMeter(new MetricName(metricGroup, operationName, "rate"),
                "requests", TimeUnit.SECONDS);
        _metricTimes = Metrics.newTimer(new MetricName(metricGroup, operationName, "times"),
                TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

        _metricSizes = includeSizes ?
                Metrics.newHistogram(new MetricName(metricGroup, operationName, "sizes"), true)
                : null;
        _metricEntryCounts = includeEntryCounts ?
                Metrics.newHistogram(new MetricName(metricGroup, operationName, "entryCount"), true)
                : null;
    }

    public static OperationMetrics forEntityOperation(ServiceConfig serviceConfig, String operationName)
    {
        return new OperationMetrics(serviceConfig, operationName, true, false);
    }

    public static OperationMetrics forListingOperation(ServiceConfig serviceConfig, String operationName)
    {
        return new OperationMetrics(serviceConfig, operationName, false, true);
    }

    public static OperationMetrics forNonPayloadOperation(ServiceConfig serviceConfig, String operationName)
    {
        return new OperationMetrics(serviceConfig, operationName, false, false);
    }
    
    public TimerContext start()
    {
        if (!_serviceConfig.metricsEnabled) {
            return null;
        }
        _metricInFlight.inc();
        _metricRate.mark();            
        return _metricTimes.time();
    }

    public void finish(TimerContext timer, OperationDiagnostics opStats)
    {
        timer.stop();
        _metricInFlight.dec();
        if (opStats != null) {
            Storable entity = opStats.getEntry();
            if (entity != null) {
                _metricSizes.update(entity.getActualUncompressedLength());
            }
        }
        if (timer != null) {
            timer.stop();
        }
    }

}
