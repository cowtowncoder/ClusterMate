package com.fasterxml.clustermate.dw;

public enum RunMode {
    /**
     * Default run mode used by non-test services: will initialize everything
     * normally
     */
    FULL(false, true),
    
    /**
     * Light-weight test mode, in which no background tasks are run; service is
     * marked as being run in test mode
     */
    TEST_MINIMAL(true, false),

    /**
     * "Heavy" test mode, in which all normal initialization is done, including
     * start up of background tasks.
     */
    TEST_FULL(true, true);

    protected final boolean _testing;
    
    protected final boolean _runTasks;

    private RunMode(boolean isTesting, boolean runTasks)
    {
        _testing = isTesting;
        _runTasks = runTasks;
    }

    public boolean isTesting() { return _testing; }
    
    public boolean shouldRunTasks() { return _runTasks; }
}
