package com.fasterxml.clustermate.client.operation;

public interface WriteOperation<T extends WriteOperationResult<T>>
{
    /**
     * Accessor that can be used to check the current state of the write operation
     */
    public T getCurrentState();
    
    /**
     * Method called to try to complete operation such that it fulfills minimal
     * succeeded nodes criteria. Will return either when enough calls have
     * succeeded, or when timeout occurs.
     * Method is allowed to make multiple rounds of retries as necessary.
     */
    public T completeMinimally() throws InterruptedException;

    /**
     * Method called to try to complete operation such that it fulfills optimal
     * succeeded nodes criteria. Will return either when enough calls have
     * succeeded, or when timeout occurs.
     * Method is allowed to make multiple rounds of retries as necessary.
     */
    public T completeOptimally() throws InterruptedException;

    /**
     * Method called to try to complete operation such that it fulfills maximal
     * succeeded nodes criteria. Will return either when enough calls have
     * succeeded, or when timeout occurs.
     * Method is allowed to make multiple rounds of retries as necessary.
     */
    public T completeMaximally() throws InterruptedException;

    /**
     * Method called to try to see if operation could fulfill maximum criteria
     * by completing the initial round of calls; but will not send retries to
     * nodes for which call has already been made.
     */
    public T tryCompleteMaximally() throws InterruptedException;
}
