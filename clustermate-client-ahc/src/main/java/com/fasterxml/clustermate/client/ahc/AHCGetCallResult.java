package com.fasterxml.clustermate.client.ahc;

import com.fasterxml.storemate.client.CallFailure;
import com.fasterxml.storemate.client.call.GetCallResult;
import com.ning.http.client.HttpResponseHeaders;

/**
 * Container for results of a single GET call to a server node.
 * Note that at most one of '_fail' and '_result' can be non-null; however,
 * it is possible for both to be null: this occurs in cases where
 * communication to server(s) succeeds, but no content was found
 * (either 404, or deleted content).
 */
public final class AHCGetCallResult<T> extends GetCallResult<T>
{
    protected HttpResponseHeaders _headers;

    /*
    ///////////////////////////////////////////////////////////////////////
    // Construction, initialization
    ///////////////////////////////////////////////////////////////////////
     */
    
    public AHCGetCallResult(int status, T result) {
        super(status, result);
    }

    public AHCGetCallResult(CallFailure fail) {
        super(fail);
    }

    public static <T> AHCGetCallResult<T> notFound() {
        return new AHCGetCallResult<T>(404, null);
    }

    public void setHeaders(HttpResponseHeaders h) {
        _headers = h;
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Accessors
    ///////////////////////////////////////////////////////////////////////
     */

    @Override
    public String getHeaderValue(String key)
    {
        HttpResponseHeaders h = _headers;
        return (h == null) ? null : h.getHeaders().getFirstValue(key);
    }

    /*
    public FluentCaseInsensitiveStringsMap getHeaders() {
        HttpResponseHeaders h = _headers;
        return (h == null) ? null : h.getHeaders();
    }
*/    
}
