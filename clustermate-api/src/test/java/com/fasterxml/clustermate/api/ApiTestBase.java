package com.fasterxml.clustermate.api;

import junit.framework.TestCase;

public class ApiTestBase extends TestCase
{
    /*
    ///////////////////////////////////////////////////////////////////////
    // Exception verification methods
    ///////////////////////////////////////////////////////////////////////
     */

    protected final void verifyException(Exception e, String expected)
    {
        verifyMessage(expected, e.getMessage());
    }
    
    protected final void verifyMessage(String expectedPiece, String actual)
    {
        if (actual == null || actual.toLowerCase().indexOf(expectedPiece.toLowerCase()) < 0) {
            fail("Expected message that contains phrase '"+expectedPiece+"'; instead got: '"
                    +actual+"'");
        }
    }
}
