package com.fasterxml.clustermate.service.store;

/**
 * Internal response objects used to indicate what happened to attempted
 * deletion.
 */
public class DeletionResult
{
    public enum Status {
        /**
         * Deletion was successfully deferred
         */
        DEFERRED,

        /**
         * Deletion was blocked for a bit, then successfully completed
         */
        COMPLETED,

        /**
         * Deletion was attempted, but failed due to an exception
         */
        FAILED,

        /**
         * Deletion was blocked, but failed to be executed within allotted time
         */
        TIMED_OUT,

        /**
         * Failure due to full blocking queue: indicates a severed internal
         * problem and should not occur.
         */
        QUEUE_FULL
        ;
    }

    private final static DeletionResult RESULT_DEFERRED = new DeletionResult(Status.DEFERRED,  null);
    private final static DeletionResult RESULT_TIMED_OUT = new DeletionResult(Status.TIMED_OUT, null);

    protected final Status _status;

    protected final Throwable _rootCause;

    protected DeletionResult(Status status, Throwable prob)
    {
        if (status == null) {
            throw new IllegalArgumentException("Can not use null status");
        }
        _status = status;
        _rootCause = prob;
    }

    public static DeletionResult forDeferred() {
        return RESULT_DEFERRED;
    }

    public static DeletionResult forCompleted() {
        return new DeletionResult(Status.COMPLETED, null);
    }

    public static DeletionResult forFail(Throwable rootCause) {
        return new DeletionResult(Status.FAILED, rootCause);
    }

    public static DeletionResult forQueueFull() {
        return new DeletionResult(Status.QUEUE_FULL, null);
    }
    
    public static DeletionResult forTimeOut() {
        return RESULT_TIMED_OUT;
    }

    public Status getStatus() {
        return _status;
    }

    public Throwable getRootCause() {
        return _rootCause;
    }
}
