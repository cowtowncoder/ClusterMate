package com.fasterxml.clustermate.client.operation;

/**
 * Entity that represents a PUT operation during its life-cycle; before, during and
 * after zero or more actual PUT calls. The reason for separate entity is to allow
 * partial completion, and subsequent continued operation, without need for callbacks.
 * It also makes it possible to hand off further processing into separate thread, by
 * first performing minimal writes, returning, and allowing caller decide next steps
 * to take (if any).
 */
public interface PutOperation
    extends WriteOperation<PutOperationResult>
{
    /**
     * Method called to complete processing for this operation. It will both
     * finalize the result information (any partially handled call sets are
     * declared either failed -- if any retriable failures -- or skipped otherwise),
     * and release any pending resources, such as content providers.
     */
    public void finish();
}
