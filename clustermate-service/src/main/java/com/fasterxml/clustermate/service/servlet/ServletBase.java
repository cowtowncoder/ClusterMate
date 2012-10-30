package com.fasterxml.clustermate.service.servlet;

import java.io.IOException;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.clustermate.service.ServiceResponse;
import com.fasterxml.clustermate.service.cluster.ClusterViewByServer;

@SuppressWarnings("serial")
public class ServletBase extends HttpServlet
{
    protected final ClusterViewByServer _clusterView;

    protected ServletBase(ClusterViewByServer clusterView)
    {
        _clusterView = clusterView;
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // REST operation end points
    ///////////////////////////////////////////////////////////////////////
     */

    @Override
    public final void doGet(HttpServletRequest req0, HttpServletResponse resp0) throws IOException
    {
        handleGet(constructRequest(req0), constructResponse(resp0));
    }

    @Override
    public final void doHead(HttpServletRequest req0, HttpServletResponse resp0) throws IOException
    {
        handleHead(constructRequest(req0), constructResponse(resp0));
    }
    
    @Override
    public final void doPost(HttpServletRequest req0, HttpServletResponse resp0) throws IOException
    {
        handlePost(constructRequest(req0), constructResponse(resp0));
    }
    
    @Override
    public final void doPut(HttpServletRequest req0, HttpServletResponse resp0) throws IOException
    {
        handlePut(constructRequest(req0), constructResponse(resp0));
    }

    @Override
    public final void doDelete(HttpServletRequest req0, HttpServletResponse resp0) throws IOException
    {
        handleDelete(constructRequest(req0), constructResponse(resp0));
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Overridable methods
    ///////////////////////////////////////////////////////////////////////
     */

    public void handleGet(ServletServiceRequest request, ServletServiceResponse response) throws IOException { }
    public void handleHead(ServletServiceRequest request, ServletServiceResponse response) throws IOException { }
    public void handlePut(ServletServiceRequest request, ServletServiceResponse response) throws IOException { }
    public void handlePost(ServletServiceRequest request, ServletServiceResponse response) throws IOException { }
    public void handleDelete(ServletServiceRequest request, ServletServiceResponse response) throws IOException { }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Helper methods, overridable
    ///////////////////////////////////////////////////////////////////////
     */

    protected ServletServiceRequest constructRequest(HttpServletRequest orig) {
     return new ServletServiceRequest(orig);
    }
    
    protected ServletServiceResponse constructResponse(HttpServletResponse orig) {
     return new ServletServiceResponse(orig);
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Helper methods, other
    ///////////////////////////////////////////////////////////////////////
     */

    protected ServiceResponse _addStdHeaders(ServiceResponse response)
    {
        if (_clusterView != null) {
            response = _clusterView.addClusterStateHeaders(response);
        }
        return response;
    }
}
