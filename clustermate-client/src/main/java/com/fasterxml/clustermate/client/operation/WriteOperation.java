package com.fasterxml.clustermate.client.operation;

public interface WriteOperation<RESULT extends WriteOperationResult<RESULT>,
      OPER extends WriteOperation<RESULT,OPER>
>
{
    /**
     * Accessor for simple, developer-visible type of this operation;
     * suitable for debugging and diagnostics messages
     */
    public String getTypeDesc();

    /**
     * Accessor that can be used to check the current state of the write operation
     */
    public RESULT result();

    /**
     * Method called to complete processing for this operation. It will both
     * finalize the result information (any partially handled call sets are
     * declared either failed -- if any retriable failures -- or skipped otherwise),
     * and release any pending resources, such as content providers.
     */
    public RESULT finish();
    
    /**
     * @return Number of call rounds completed currently
     */
    public int completedRounds();

    /**
     * Method for checking whether we may make any more calls for this operation;
     * returns false if no nodes exist for which we can make calls.
     *<p>
     * Functionally equivalent to:
     *<pre>
     *    remainingHostCount() &gt; 0;
     *</pre>
     * 
     * @return True if calling one of "completeXxx()" may result in calls being made;
     *   false if it is known that no more calls may be made.
     */
    public boolean hasRemainingHosts();
    
    /**
     * @return Number of hosts that may still be called by this operation; zero
     *    when operation is complete with combination of successes and/or
     *    non-retriable failures.
     */
    public int remainingHostCount();
    
    /**
     * Method called to try to complete operation such that it fulfills minimal
     * succeeded nodes criteria. Will return either when enough calls have
     * succeeded, or when timeout occurs.
     * Method is allowed to make multiple rounds of retries as necessary.
     */
    public OPER completeMinimally() throws InterruptedException;

    /**
     * Method called to try to complete operation such that it fulfills optimal
     * succeeded nodes criteria. Will return either when enough calls have
     * succeeded, or when timeout occurs.
     * Method is allowed to make multiple rounds of retries as necessary.
     */
    public OPER completeOptimally() throws InterruptedException;

    /**
     * Method called to try to complete operation such that it fulfills maximal
     * succeeded nodes criteria. Will return either when enough calls have
     * succeeded, or when timeout occurs.
     * Method is allowed to make multiple rounds of retries as necessary.
     */
    public OPER completeMaximally() throws InterruptedException;

    /**
     * Method called to try to see if operation could fulfill maximum criteria
     * by completing the initial round of calls; but will not send retries to
     * nodes for which call has already been made.
     */
    public OPER tryCompleteMaximally() throws InterruptedException;
}
