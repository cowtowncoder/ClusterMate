package com.fasterxml.clustermate.api;

/**
 * Interface implemented by server-side request objects, to allow
 * dispatching of requests.
 * Note that all values are handled as-is, without doing any URL decoded (or
 * encoding).
 */
public interface DecodableRequestPath
{
    /**
     * Method for returning current path in its entirety (and without URL decoding).
     * Usually used to be able to later on reset path with setPath().
     */
    public String getPath();

    /**
     * Method for resetting currently active path.
     */
    public void setPath(String fullPath);

    /**
     * Method for checking the next available path segment,
     * if any are left, removing ("consume") it, and returning to caller.
     */
    public String nextPathSegment();

    /**
     * Method for checking that the next path segment (as returned by
     * {@link #peekPathSegment}) would match given segment; and if so,
     * remove the segment and return true. Otherwise returns null
     * and leaves path unchanged.
     */
    public boolean matchPathSegment(String segment);
}
