package com.fasterxml.clustermate.servlet;

import java.io.IOException;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.clustermate.service.ServiceResponse;
import com.fasterxml.clustermate.service.SharedServiceStuff;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;
import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.util.OperationDiagnostics;

@SuppressWarnings("serial")
public class ServletBase extends HttpServlet
{
    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    protected final TimeMaster _timeMaster;
    
    protected final ClusterViewByServer _clusterView;

    protected final String _basePath;
    
    /**
     * @param clusterView Handler to cluster information; needed to
     *   handling piggy-backed cluster state information
     * @param servletPathBase (optional) Server base path that is to
     *   be ignored when matching request paths: if null, uses
     *   method <code>getServletPath</code> of <code>HttpServletRequest</code>.
     */
    protected ServletBase(SharedServiceStuff stuff, ClusterViewByServer clusterView,
            String servletPathBase)
    {
        _timeMaster = stuff.getTimeMaster();
        _clusterView = clusterView;
        if (servletPathBase != null) {
            /* One tweak: ensure that base path ends with slash, to ensure
             * we are not left with leading slash in path passed.
             * Except in case of empty base.
             */
            if (!servletPathBase.endsWith("/") && servletPathBase.length() > 0) {
                servletPathBase += "/";
            }
        }
        _basePath = servletPathBase;
    }

    protected ClusterViewByServer getClusterView() { return _clusterView; }
    
    /*
    /**********************************************************************
    /* REST operation end points
    /**********************************************************************
     */

    @Override
    public final void doGet(HttpServletRequest req0, HttpServletResponse resp0) throws IOException
    {
        handleGet(constructRequest(req0), constructResponse(resp0), constructMetadata());
    }

    @Override
    public final void doHead(HttpServletRequest req0, HttpServletResponse resp0) throws IOException
    {
        handleHead(constructRequest(req0), constructResponse(resp0), constructMetadata());
    }
    
    @Override
    public final void doPost(HttpServletRequest req0, HttpServletResponse resp0) throws IOException
    {
        handlePost(constructRequest(req0), constructResponse(resp0), constructMetadata());
    }
    
    @Override
    public final void doPut(HttpServletRequest req0, HttpServletResponse resp0) throws IOException
    {
        handlePut(constructRequest(req0), constructResponse(resp0), constructMetadata());
    }

    @Override
    public final void doDelete(HttpServletRequest req0, HttpServletResponse resp0) throws IOException
    {
        handleDelete(constructRequest(req0), constructResponse(resp0), constructMetadata());
    }

    /*
    /**********************************************************************
    /* Overridable methods
    /**********************************************************************
     */

    public void handleGet(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats)
        throws IOException
    {
        response = response.badMethod()
                .setContentTypeText().setEntity("No GET available for endpoint");
        // no need to track bytes returned since it's not real payload
        response.writeOut(null);
    }

    public void handleHead(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats)
        throws IOException { }

    public void handlePut(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats)
        throws IOException { }

    public void handlePost(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats)
        throws IOException { }

    public void handleDelete(ServletServiceRequest request, ServletServiceResponse response,
            OperationDiagnostics stats)
        throws IOException { }

    /*
    /**********************************************************************
    /* Helper methods, overridable
    /**********************************************************************
     */

    protected ServletServiceRequest constructRequest(HttpServletRequest orig)
    {
        String path = orig.getRequestURI();
        
        /* First things first: remove servlet path prefix (either override,
         * or one used for registration)
         */
        if (_basePath == null) { // no base path; use whatever it was registered with
            path = _trimPath(path, orig.getServletPath(), true);
        } else {
            path = _trimPath(path, _basePath, false);
        }
        // false -> path not yet URL decoded
        return new ServletServiceRequest(orig, path, false);
    }

    /**
     * Overridable factory method that is used for creating optional
     * {@link OperationDiagnostics} object to pass through, to collect
     * statistics.
     */
    protected OperationDiagnostics constructMetadata() {
        /* 28-Mar-2013, tsaloranta: Let's construct this by default, since
         *   it will be needed to collect metrics.
         */
        return new OperationDiagnostics(_timeMaster.nanosForDiagnostics());
    }
    
    protected ServletServiceResponse constructResponse(HttpServletResponse orig) {
        return new ServletServiceResponse(orig);
    }
    
    /*
    /**********************************************************************
    /* Helper methods, other
    /**********************************************************************
     */

    protected String _trimPath(String originalPath, String expectedPrefix,
            boolean checkSlash)
    {
        int prefixLength = expectedPrefix.length();
        if (prefixLength == 0) {
            return originalPath;
        }
        if (!originalPath.startsWith(expectedPrefix)) {
            LOG.warn("Unexpected Servlet path to trim '{}'; does not start with '{}'; passing as-is",
                    originalPath, expectedPrefix);
            return originalPath;
        }
        // need to trim one char, slash?
        if (checkSlash
                && (originalPath.length() > prefixLength)
                && (expectedPrefix.charAt(prefixLength-1) != '/')
                && (originalPath.charAt(prefixLength) == '/')
        ) {
            ++prefixLength;
        }
        return originalPath.substring(prefixLength);
    }
    
    protected ServiceResponse _addStdHeaders(ServiceResponse response)
    {
        if (_clusterView != null) {
            response = _clusterView.addClusterStateInfo(response);
        }
        return response;
    }
}
