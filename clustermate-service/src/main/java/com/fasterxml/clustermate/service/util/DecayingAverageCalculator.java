package com.fasterxml.clustermate.service.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Helper class used for calculating approximate moving averages
 * that emphasize most recent samples and use exponential decay.
 * The main benefit of this approach is (besides favoring most
 * recent values over older ones) is that amount of state kept
 * is minimal: simply a single average value.
 *<p>
 * Calculation is done by simple: 'avgN+1 = (p * newSample) + (1.0 - p) * avgN'
 * formula; where 'p' is '1/len' and 'len' is the length of sample window
 * chosen when constructing instance.
 *<p>
 * NOTE: samples are assumed to be all positive or all positive (that is; should
 * not mix both negative and positive numbers). This matters with truncation;
 * otherwise calculations should work even with mixed values.
 */
public class DecayingAverageCalculator
{
    protected final double _newSampleMultiplier, _oldAvgMultiplier;

    protected final double _maxSampleMultiplier;

    protected double _currentAverage;

    protected final AtomicInteger _roundedAverage;
    
    /**
     * Lock we use to guard updates of the current average
     */
    protected final Object _lock = new Object();

    /**
     * 
     * @param len Length of logical sample window; used for calculating decay
     *   multiplier (1 / len)
     * @param initialEstimate Initial average value to use
     * @param maxMultiplier Multiplier value used for truncating possible outlier
     *    values; new samples are truncated to 'maxMultiplier * currentAverage'
     *    (i.e. can not exceed thus limit)
     */
    public DecayingAverageCalculator(int len, int initialEstimate,
            double maxMultiplier)
    {
        if (len < 2) {
            throw new IllegalArgumentException("'len' can not be less than 2");
        }
        // must be positive, exceed 1.0
        if (maxMultiplier <= 1.0) {
            throw new IllegalArgumentException("'maxMultiplier' must exceed 1.0");
        }
        _newSampleMultiplier = 1.0 / (double) len;
        _oldAvgMultiplier = 1.0 - _newSampleMultiplier;
        _maxSampleMultiplier = maxMultiplier;

        _roundedAverage = new AtomicInteger(initialEstimate);
        _currentAverage = initialEstimate;
    }

    public void addSample(int sample)
    {
        synchronized (_lock) {
            final double old = _currentAverage;
            double truncatedSample = Math.min(old * _maxSampleMultiplier, (double) sample);
            double newAvg = (_newSampleMultiplier * truncatedSample) + (_oldAvgMultiplier * old);
            
            _currentAverage = newAvg;
            _roundedAverage.set((int) Math.round(newAvg));
        }
    }

    public int getCurrentAverage() {
        return _roundedAverage.get();
    }
}
