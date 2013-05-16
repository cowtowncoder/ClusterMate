package com.fasterxml.clustermate.service.metrics;

public class AllOperationMetrics
{
    public OperationMetrics GET;
    public OperationMetrics PUT;
    public OperationMetrics DELETE;

    public static interface Provider {
        public AllOperationMetrics getOperationMetrics();
    }
}
