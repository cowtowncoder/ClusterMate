package com.fasterxml.clustermate.client.operation;

import com.fasterxml.clustermate.client.call.PutContentProvider;

/**
 * Entity that represents a PUT operation during its life-cycle; before, during and
 * after zero or more actual PUT calls. The reason for separate entity is to allow
 * partial completion, and subsequent continued operation, without need for callbacks.
 * It also makes it possible to hand off further processing into separate thread, by
 * first performing minimal writes, returning, and allowing caller decide next steps
 * to take (if any).
 */
public interface PutOperation
    extends WriteOperation<PutOperationResult, PutOperation>
{
    /**
     * Accessor for content to be PUT; usually only needed for debugging or
     * advanced functionality (aka "hacks").
     */
    public PutContentProvider content();
}
