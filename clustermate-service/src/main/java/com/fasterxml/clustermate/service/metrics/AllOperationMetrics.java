package com.fasterxml.clustermate.service.metrics;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({ "GET", "PUT", "DELETE", "INFO", "LIST", "SYNCLIST", "SYNCPULL" })
public class AllOperationMetrics
{
    public ExternalOperationMetrics GET;
    public ExternalOperationMetrics PUT;
    public ExternalOperationMetrics DELETE;

    public ExternalOperationMetrics INFO;
    public ExternalOperationMetrics LIST;

    public ExternalOperationMetrics SYNCLIST;
    public ExternalOperationMetrics SYNCPULL;
    
    public static interface Provider {
        public void fillOperationMetrics(AllOperationMetrics metrics);
    }
}
