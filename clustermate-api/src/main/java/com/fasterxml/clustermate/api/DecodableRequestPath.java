package com.fasterxml.clustermate.api;

/**
 * Interface implemented by server-side request objects, to allow
 * routing/dispatching of requests based on information from path
 * and related target metadata.
 * Note that all values are handled as-is, without doing any URL decoded (or
 * encoding).
 *<p>
 * Note: typically implemented by server-side request objects.
 */
public interface DecodableRequestPath
{
    /**
     * Method for returning current remaning path (not including
     * parts that have been removed with {@link #nextPathSegment()});
     * and without additional URL decoding (but may have been decoded,
     * as per {@link #getDecodedPath()}).
     * Usually used to be able to later on reset path with setPath().
     */
    public String getPath();

    /**
     * Method for returning current remaining path, but first
     * URL decoding it if necessary (if {@link #isPathDecoded} returns
     * false).
     */
    public String getDecodedPath();
    
    /**
     * Whether path as returned by {@link #getPath} has already been
     * URL decoded or not.
     */
    public boolean isPathDecoded();

    /**
     * Method for resetting currently active path. Note that the value
     * must obey same "has been URL decoded" value as what
     * {@link #isPathDecoded()} returns; not encoding or decoding is done
     * by this method.
     */
    public void setPath(String fullPath);

    /**
     * Method for checking the next available path segment,
     * if any are left, removing ("consume") it, and returning to caller.
     */
    public String nextPathSegment();

    /**
     * Method for checking that the next path segment (as returned by
     * {@link #nextPathSegment}) would match given segment; and if so,
     * remove the segment and return true. Otherwise returns null
     * and leaves path unchanged.
     */
    public boolean matchPathSegment(String segment);

    /**
     * Accessor for getting value of specified query parameter
     */
    public abstract String getQueryParameter(String key);

    /**
     * Accessor for getting value of specified request header
     */
    public abstract String getHeader(String key);
}
