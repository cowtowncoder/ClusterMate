package com.fasterxml.clustermate.jaxrs;

import java.io.InputStream;

import javax.ws.rs.*;
import javax.ws.rs.core.*;

import com.yammer.metrics.annotation.Timed;

import com.fasterxml.storemate.shared.EntryKey;
import com.fasterxml.storemate.store.StoreException;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.api.OperationType;
import com.fasterxml.clustermate.service.store.StoredEntry;
import com.fasterxml.clustermate.service.sync.SyncHandler;

/**
 * Resource that implements interface used by server-to-server
 * synchronization functionality, for transferring data by
 * the "anti-entropy" process.
 *<p>
 * For testing, try accessing: http://localhost:9090/v/sync/list/0?keyRangeStart=0&keyRangeLength=360
 * with browser.
 */
@Path("/v/sync/")
public class SyncResource<K extends EntryKey, E extends StoredEntry<K>>
{
    protected final SyncHandler<K,E> _syncHandler;
    
    /*
    /**********************************************************************
    /* Life-cycle
    /**********************************************************************
     */
    
    public SyncResource(SyncHandler<K,E> h) {
        _syncHandler = h;
    }
        
    /*
    /**********************************************************************
    /* API, file listing
    /**********************************************************************
     */
    
    /**
     * End point clients use to find out metadata for entries this node has,
     * starting with the given timestamp.
     */
    @GET
    @Path("list")
    @Produces({ MediaType.APPLICATION_JSON, ClusterMateConstants.CONTENT_TYPE_SMILE })
    @Timed
    public Response listEntries(@Context UriInfo uriInfo, @Context HttpHeaders headers,
            @QueryParam("since") Long sinceL)
    	throws StoreException
    {
        JaxrsHttpResponse response = new JaxrsHttpResponse();
        _syncHandler.listEntries(new JaxrsHttpRequest(uriInfo, headers, "", OperationType.GET),
                response, sinceL);
        return response.buildResponse();
    }
    
    /*
    /**********************************************************************
    /* API, direct content download
    /* (no (un)compression etc)
    /**********************************************************************
     */

    /**
     * Access endpoint used by others nodes to 'pull' data for entries they are
     * missing.
     * Note that request payload must be JSON; may change to Smile in future if
     * need be.
     */
    @POST
    @Path("pull")
    @Timed
    public Response pullEntries(@Context UriInfo uriInfo, @Context HttpHeaders headers,
            InputStream in)
    	throws StoreException
    {
        JaxrsHttpResponse response = new JaxrsHttpResponse();
        _syncHandler.pullEntries(new JaxrsHttpRequest(uriInfo, headers, "", OperationType.POST), response, in);
        return response.buildResponse();
    }
}
