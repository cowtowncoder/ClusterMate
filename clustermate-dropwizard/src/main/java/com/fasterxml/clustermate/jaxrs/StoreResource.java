package com.fasterxml.clustermate.jaxrs;

import java.io.*;

import javax.ws.rs.*;
import javax.ws.rs.core.*;

import com.yammer.metrics.annotation.Timed;

import com.fasterxml.storemate.shared.EntryKey;
import com.fasterxml.storemate.store.StoreException;

import com.fasterxml.clustermate.api.EntryKeyConverter;
import com.fasterxml.clustermate.api.OperationType;
import com.fasterxml.clustermate.service.ServiceResponse;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.Stores;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.clustermate.service.store.StoreHandler;
import com.fasterxml.clustermate.service.store.StoredEntry;

/**
 * Main resource to interface with storage system when deploying on
 * a JAX-RS container, exposing basic CRUD (PUT, GET, DELETE)
 * entry points.
 */
@Path("/v/store/entry")
public abstract class StoreResource<K extends EntryKey, E extends StoredEntry<K>>
{
    /*
    /**********************************************************************
    /* Helper objects
    /**********************************************************************
     */
    
    protected final StoreHandler<K, E> _storeHandler;

    protected final ClusterViewByServer _clusterView;

    protected final EntryKeyConverter<K> _keyConverter;
    
    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */
    
    public StoreResource(SharedServiceStuff stuff, ClusterViewByServer clusterView,
            StoreHandler<K, E> storeHandler)
    {
        _storeHandler = storeHandler;
        _clusterView = clusterView;
        _keyConverter = stuff.getKeyConverter();
    }

    /*
    /**********************************************************************
    /* Helper methods for unit tests
    /**********************************************************************
     */

    /**
     * Method to be only used by unit tests...
     */
    public StoreHandler<K,E> getHandler() {
        return _storeHandler;
    }

    public Stores<K,E> getStores() {
        return _storeHandler.getStores();
    }

    public ClusterViewByServer getCluster() {
        return _clusterView;
    }
    
    /*
    /**********************************************************************
    /* Content creation
    /**********************************************************************
     */
    
    @PUT @Timed
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{externalPath: .*}")
    public Response putPrimary(@Context UriInfo uriInfo, @Context HttpHeaders headers,
            @PathParam("externalPath") String externalPath,
            InputStream dataIn)
    	throws IOException, StoreException
    {
        return handlePut(new JaxrsHttpRequest(uriInfo, headers, externalPath, OperationType.PUT),
                dataIn);
    }

    // Alias for PUT -- why not!
    @POST @Timed
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{externalPath: .*}")
    public Response postPrimary(@Context UriInfo uriInfo, @Context HttpHeaders headers,
            @PathParam("externalPath") String externalPath,
            InputStream dataIn)
        throws IOException, StoreException
    {
        return handlePut(new JaxrsHttpRequest(uriInfo, headers, externalPath, OperationType.POST), dataIn);
    }
    
    protected final Response handlePut(JaxrsHttpRequest request, InputStream dataIn)
        throws IOException, StoreException
    {
        JaxrsHttpResponse response = new JaxrsHttpResponse();
        K key = _findKey(request, response);
        if (key != null) {
            _storeHandler.putEntry(request, response, key, dataIn);
            // for all requests, or just non-fail ones?
            _addStdHeaders(response);
        }
        return response.buildResponse();
    }

    /*
    /**********************************************************************
    /* Content lookups
    /**********************************************************************
     */

    @GET
    @Path("{externalPath: .*}")
    @Timed
    // can't define content type: successful requests give 'raw' type; errors JSON
    public Response getPrimary(@Context UriInfo uriInfo, @Context HttpHeaders headers,
            @PathParam("externalPath") String externalPath)
                    throws StoreException
    {
        JaxrsHttpRequest request = new JaxrsHttpRequest(uriInfo, headers, externalPath, OperationType.GET);
        JaxrsHttpResponse response = new JaxrsHttpResponse();
        K key = _findKey(request, response);
        if (key != null) {
            _storeHandler.getEntry(request, response, key);
            // for all requests, or just non-fail ones?
            _addStdHeaders(response);
        }
        return response.buildResponse();
    }

    @HEAD
    @Path("{externalPath: .*}")
    @Timed
    // no content type, as per GET
    public Response headPrimary(@Context UriInfo uriInfo, @Context HttpHeaders headers,
            @PathParam("externalPath") String externalPath)
                    throws StoreException
    {
        JaxrsHttpRequest request = new JaxrsHttpRequest(uriInfo, headers, externalPath, OperationType.HEAD);
        JaxrsHttpResponse response = new JaxrsHttpResponse();
        K key = _findKey(request, response);
        if (key != null) {
            _storeHandler.getEntryStats(request, response, key);
            _addStdHeaders(response);
        }
        return response.buildResponse();
    }

    /*
    /**********************************************************************
    /* Content removal
    /**********************************************************************
     */

    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{externalPath: .*}")
    @Timed
    public Response deletePrimary(@Context UriInfo uriInfo, @Context HttpHeaders headers,
            @PathParam("externalPath") String externalPath)
    	throws IOException, StoreException
    {
        JaxrsHttpRequest request = new JaxrsHttpRequest(uriInfo, headers, externalPath, OperationType.DELETE);
        JaxrsHttpResponse response = new JaxrsHttpResponse();
        K key = _findKey(request, response);
        if (key != null) {
            _storeHandler.removeEntry(request, response, key);
            _addStdHeaders(response);
        }
        return response.buildResponse();
    }

    /*
    /**********************************************************************
    /* Internal methods
    /**********************************************************************
     */

    protected K _findKey(JaxrsHttpRequest request, JaxrsHttpResponse response) {
        return _keyConverter.extractFromPath(request);
    }
    
    protected ServiceResponse _addStdHeaders(ServiceResponse response)
    {
        if (_clusterView != null) {
            response = _clusterView.addClusterStateHeaders(response);
        }
        return response;
    }
}

