package com.fasterxml.clustermate.service.metrics;

public class AllOperationMetrics
{
    public ExternalOperationMetrics GET;
    public ExternalOperationMetrics PUT;
    public ExternalOperationMetrics DELETE;

    public static interface Provider {
        public AllOperationMetrics getOperationMetrics();
    }
}
