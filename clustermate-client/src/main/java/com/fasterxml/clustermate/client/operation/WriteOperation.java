package com.fasterxml.clustermate.client.operation;

public interface WriteOperation<RESULT extends WriteOperationResult<RESULT>,
      OPER extends WriteOperation<RESULT,OPER>
>
{
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
