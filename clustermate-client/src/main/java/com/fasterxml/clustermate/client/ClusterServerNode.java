package com.fasterxml.clustermate.client;

import com.fasterxml.clustermate.api.EntryKey;
import com.fasterxml.clustermate.api.KeyRange;
import com.fasterxml.clustermate.api.RequestPathBuilder;
import com.fasterxml.clustermate.client.call.ContentDeleter;
import com.fasterxml.clustermate.client.call.ContentGetter;
import com.fasterxml.clustermate.client.call.ContentHeader;
import com.fasterxml.clustermate.client.call.ContentPutter;
import com.fasterxml.clustermate.client.call.EntryLister;

/**
 * Representation of a server node that is part of a cluster, including
 * read-only accessors to state.
 */
public interface ClusterServerNode
    extends ServerNode
{
    /*
    /**********************************************************************
    /* Basic state accessors
    /**********************************************************************
     */

    /**
     * Whether server node is disabled: usually occurs during shutdowns
     * and startups, and is considered a transient state. Clients typically
     * try to avoid GET access from disabled nodes; and schedule updates
     * (if any) after all enabled instances.
     */
    public boolean isDisabled();

    /*
    /**********************************************************************
    /* Key range access
    /**********************************************************************
     */
    
    public KeyRange getActiveRange();
    public KeyRange getPassiveRange();
    public KeyRange getTotalRange();

    /*
    /**********************************************************************
    /* Timestamp access
    /**********************************************************************
     */
    
    /**
     * Timestamp when last node state access request was sent.
     */
    public long getLastRequestSent();

    /**
     * Timestamp when last node state access response was received (note:
     * does NOT include cases where error occured during request).
     */
    public long getLastResponseReceived();

    /**
     * Timestamp when last node state response update was processed.
     */
    public long getLastNodeUpdateFetched();

    /**
     * Timestamp of the last update that has been fetched from the server node.
     */
    public long getLastClusterUpdateFetched();

    /**
     * Timestamp of the latest update for the server node.
     */
    public long getLastClusterUpdateAvailable();

    /*
    /**********************************************************************
    /* Call accessors, paths etc
    /**********************************************************************
     */
    
    /**
     * Accessor for getting path builder initialized to the root path of
     * the service for this node; used for building paths to access
     * things like entries and node state.
     */
    public <B extends RequestPathBuilder<B>> B rootPath();

    public abstract <K extends EntryKey> ContentPutter<K> entryPutter();

    public abstract <K extends EntryKey> ContentGetter<K> entryGetter();

    public abstract <K extends EntryKey> ContentHeader<K> entryHeader();

    public abstract <K extends EntryKey> ContentDeleter<K> entryDeleter();

    public abstract <K extends EntryKey> EntryLister<K> entryLister();
}
