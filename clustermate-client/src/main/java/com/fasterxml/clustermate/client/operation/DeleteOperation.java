package com.fasterxml.clustermate.client.operation;

/**
 * Entity that represents a DELETE operation during its life-cycle; before, during and
 * after zero or more actual DELETE calls. The reason for separate entity is to allow
 * partial completion, and subsequent continued operation, without need for callbacks.
 * It also makes it possible to hand off further processing into separate thread, by
 * first performing minimal deletes, returning, and allowing caller decide next steps
 * to take (if any).
 */
public interface DeleteOperation
    extends WriteOperation<DeleteOperationResult, DeleteOperation>
{

}
