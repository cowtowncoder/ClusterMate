package com.fasterxml.clustermate.service.metrics;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({ "GET", "PUT", "DELETE", "INFO", "LIST",
    "SYNCLIST", "SYNCPULL",
    "REMOTE_SL", "REMOTE_SP" })
public class AllOperationMetrics
{
    public ExternalOperationMetrics GET;
    public ExternalOperationMetrics PUT;
    public ExternalOperationMetrics DELETE;

    public ExternalOperationMetrics INFO;
    public ExternalOperationMetrics LIST;

    public ExternalOperationMetrics SYNCLIST;
    public ExternalOperationMetrics SYNCPULL;

    public ExternalOperationMetrics REMOTE_SL;
    public ExternalOperationMetrics REMOTE_SP;
    
    public static interface Provider {
        public void fillOperationMetrics(AllOperationMetrics metrics);
    }
}
