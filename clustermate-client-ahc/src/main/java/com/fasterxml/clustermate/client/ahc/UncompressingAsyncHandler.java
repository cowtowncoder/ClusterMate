package com.fasterxml.clustermate.client.ahc;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

import com.ning.compress.DataHandler;
import com.ning.compress.Uncompressor;
import com.ning.compress.UncompressorOutputStream;
import com.ning.compress.gzip.GZIPUncompressor;
import com.ning.compress.lzf.LZFUncompressor;

import com.ning.http.client.AsyncHandler;
import com.ning.http.client.FluentCaseInsensitiveStringsMap;
import com.ning.http.client.HttpResponseBodyPart;
import com.ning.http.client.HttpResponseHeaders;
import com.ning.http.client.HttpResponseStatus;

import com.fasterxml.clustermate.api.ClusterMateConstants;
import com.fasterxml.clustermate.client.call.GetContentProcessor;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.shared.util.ByteAggregator;
import com.fasterxml.storemate.shared.util.IOUtil;


/**
 * Translator class that implements {@link AsyncHandler} but
 * calls {@link GetContentProcessor} with contents.
 *
 * @param <T> Result type of actual GET content handler we are constructed with
 */
public class UncompressingAsyncHandler<T>
    implements AsyncHandler<T>, DataHandler
{
    /**
     * Maximum number of bytes we will read to get an excerpt for failed requests.
     */
    protected final static int MAX_EXCERPT_LENGTH = 1000;
    
    /**
     * Handler to delegate content calls to.
     */
    protected final GetContentProcessor.Handler<T> _handler;

    /**
     * Stream wrapper used for passing content; wraps either uncompressor
     * or handler.
     */
    protected OutputStream _streamAdapter;

    /**
     * HTTP status code received, if any
     */
    protected int _status = 0;

    /**
     * Flag to indicate a failed call; response content of such calls is only
     * partially collector (for error excerpt), and no result Object will
     * be created.
     */
    protected boolean _failed;
    
    /**
     * Response headers received, if any
     */
    protected FluentCaseInsensitiveStringsMap _headers;

    /**
     * Partial aggregated content read from response sent by failed requests.
     */
    protected ByteAggregator _failExcerpt;
    
    protected String _failExcerptString;
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // Construction
    ///////////////////////////////////////////////////////////////////////
     */
    
    public UncompressingAsyncHandler(GetContentProcessor<T> proc) {
        _handler = proc.createHandler();
    }

    public UncompressingAsyncHandler(GetContentProcessor.Handler<T> h) {
        _handler = h;
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Public accessors
    ///////////////////////////////////////////////////////////////////////
     */

    public boolean isFailed() { return _failed; }
    
    public int getStatus() { return _status; }

    public FluentCaseInsensitiveStringsMap getHeaders() {
        return _headers;
    }

    public String getExcerpt()
    {
        String msg = _failExcerptString;
        if (msg == null) {
            if (_failExcerpt == null) {
                msg = "[no excerpt available]";
            } else {
                try {
                    msg = new String(_failExcerpt.toByteArray(), "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException("Internal error: could not do UTF-8 decoding: "+e, e);
                }
            }
            _failExcerptString = msg;
        }
        return msg;
    }
    
    /*
    ///////////////////////////////////////////////////////////////////////
    // AsyncHandler for AHC
    ///////////////////////////////////////////////////////////////////////
     */
    
    @Override
    public com.ning.http.client.AsyncHandler.STATE onBodyPartReceived(HttpResponseBodyPart part)
            throws Exception
    {
        // One complication: for failed calls, we take (partial) excerpt
        if (_failExcerpt != null) {
            byte[] data = part.getBodyPartBytes();
            int needed = _failExcerpt.size() - MAX_EXCERPT_LENGTH;
            if (needed > 0) {
                _failExcerpt.write(data, 0, Math.min(needed, data.length));
            }
            
            // and abort request as soon as we got as much as we needed
            if (_failExcerpt.size() < MAX_EXCERPT_LENGTH) {
                return STATE.CONTINUE;
            }
            return STATE.ABORT;
        }
        
        part.writeTo(_streamAdapter);
        return STATE.CONTINUE;
    }

    @Override
    public T onCompleted() throws Exception {
        return _handler.completeContentProcessing();
    }

    @Override
    public com.ning.http.client.AsyncHandler.STATE onHeadersReceived(HttpResponseHeaders h)
        throws Exception
    {
        FluentCaseInsensitiveStringsMap headers = (h == null) ? null : h.getHeaders();
        _headers = headers;
        String comps = (headers == null) ? null : headers.getFirstValue(ClusterMateConstants.HTTP_HEADER_COMPRESSION);
        if (comps != null && !comps.isEmpty()) {
            Uncompressor uncomp = null;
            Compression c = Compression.from(comps);
            if (c == Compression.LZF) {
                uncomp = new LZFUncompressor(this);
            } else if (c == Compression.GZIP) {
                uncomp = new GZIPUncompressor(this);
            } else if (c == Compression.NONE) {
                ; // not compressed, fine as is
            } else {
                throw new IOException("Unrecognized/unsupported compression type '"+comps+"': only 'gzip' and 'lzf' supported");
            }
            // adapter wraps uncompressor as OutputStream; uncompressor calls "handleData"
            // on this handler... a few layers of abstraction.
            _streamAdapter = new UncompressorOutputStream(uncomp);
        } else {
            // if no uncompression needed, still need to wrap async handler as an OutputStream
            // to efficiently get contents from BodyPart
            _streamAdapter = _handler.asStream();
        }
        return STATE.CONTINUE;
    }

    @Override
    public com.ning.http.client.AsyncHandler.STATE onStatusReceived(HttpResponseStatus status)
        throws Exception
    {
        _status = status.getStatusCode();
        boolean fail = !IOUtil.isHTTPSuccess(_status);
        _failed = fail;
        if (fail) {
            _failExcerpt = new ByteAggregator(MAX_EXCERPT_LENGTH);
        }
        return STATE.CONTINUE;
    }

    @Override
    public void onThrowable(Throwable t) {
        _handler.contentProcessingFailed(t);
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // DataHandler (for Uncompressor)
    ///////////////////////////////////////////////////////////////////////
     */
    
    /**
     * Method called by uncompressor; needs to dispatch content call appropriately.
     */
    @Override
    public void handleData(byte[] buffer, int offset, int len) throws IOException {
        _handler.processContent(buffer, offset, len);
    }

    @Override
    public void allDataHandled() throws IOException {
    }
}
