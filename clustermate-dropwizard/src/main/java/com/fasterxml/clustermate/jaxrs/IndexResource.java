package com.fasterxml.clustermate.jaxrs;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * Resource that handles access to index file(s) (root page) and favicon.
 */
@Produces(MediaType.TEXT_HTML)
@Path("")
public class IndexResource
{
    private final byte[] _indexContents;
    private final byte[] _faviconContents;

    public IndexResource(byte[] index, byte[] favicon)
    {
        _indexContents = index;
        _faviconContents = favicon;
    }
	
    @GET
    @Path("/index.html")
    public byte[] indexHtml() { return index(); }

    @GET
    @Path("/")
    public byte[] std() { return index(); }

    @GET
    @Path("/index.htm")
    public byte[] indexHtm() { return index(); }

    @GET
    @Path("/favicon.ico")
    public byte[] getFavicon() { return favicon(); }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Helper methods
    ///////////////////////////////////////////////////////////////////////
     */

    private byte[] index() {
        return _indexContents;
    }

    private byte[] favicon() {
        return _faviconContents;
    }
}
