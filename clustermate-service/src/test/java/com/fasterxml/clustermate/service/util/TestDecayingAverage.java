package com.fasterxml.clustermate.service.util;

import junit.framework.TestCase;

public class TestDecayingAverage extends TestCase
{
    public void testSimple()
    {
        // start with simple half-and-half variant...
        DecayingAverageCalculator calc = new DecayingAverageCalculator(2, 10, 5.0);
        assertEquals(10, calc.getCurrentAverage());
        calc.addSample(8); // (10 + 8) / 2 == 9
        assertEquals(9, calc.getCurrentAverage());
        calc.addSample(5); // (9 + 5) / 2 == 7
        assertEquals(7, calc.getCurrentAverage());
        calc.addSample(5); // (7 + 5) / 2 == 6
        assertEquals(6, calc.getCurrentAverage());
        calc.addSample(12); // (6 + 12) / 2 == 9
        assertEquals(9, calc.getCurrentAverage());
    }

    public void testTruncation()
    {
        DecayingAverageCalculator calc = new DecayingAverageCalculator(2, 10, 2.0);
        assertEquals(10, calc.getCurrentAverage());
        // more than twice current estimate should get trunc'd
        calc.addSample(30); // (10 + 20) / 2 == 15
        assertEquals(15, calc.getCurrentAverage());
        // but now sample is within bounds so:
        calc.addSample(30); // (15 + 30) / 2 == 22.5 ~= 23
        assertEquals(23, calc.getCurrentAverage());
    }

    public void testLonger()
    {
        DecayingAverageCalculator calc = new DecayingAverageCalculator(4, 10, 10.0);
        assertEquals(10, calc.getCurrentAverage());
        calc.addSample(20); // (0.75 * 10) + (0.25 * 20) -> 12.5
        assertEquals(13, calc.getCurrentAverage());
        calc.addSample(10); // -> (0.75 * 12.5) + (0.25 * 10) -> 11.87
        assertEquals(12, calc.getCurrentAverage());
    }
}
