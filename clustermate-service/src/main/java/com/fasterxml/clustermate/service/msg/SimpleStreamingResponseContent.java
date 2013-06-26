package com.fasterxml.clustermate.service.msg;

import java.io.*;

import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.shared.ByteRange;
import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.store.util.OperationDiagnostics;

/**
 * Simple implementation of {@link StreamingResponseContent} that is used
 * for handling inlined entries.
 */
public class SimpleStreamingResponseContent
    implements StreamingResponseContent
{
    /*
    /**********************************************************************
    /* Data to stream out
    /**********************************************************************
     */

    private final OperationDiagnostics _diagnostics;

    private final TimeMaster _timeMaster;

    private final ByteContainer _data;
     
    private final long _dataOffset;
     
    private final long _dataLength;
    
    /*
    /**********************************************************************
    /* Metadata
    /**********************************************************************
     */

    /**
     * Content length as reported when caller asks for it; -1 if not known.
     */
    private final long _contentLength;

    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */

    /*
        final long writeStartNanos = (diags == null) ? 0L : _timeMaster.nanosForDiagnostics();
     */
    
    public SimpleStreamingResponseContent(OperationDiagnostics diag, TimeMaster timeMaster,
            ByteContainer data, ByteRange range, long contentLength)
    {
        _diagnostics = diag;
        _timeMaster = timeMaster;
        if (data == null) {
            throw new IllegalArgumentException();
        }
        _data = data;
        // Range request? let's tweak offsets if so...
        if (range == null) {
            _dataOffset = -1L;
            _dataLength = -1L;
            _contentLength = contentLength;
        } else {
            _dataOffset = range.getStart();
            _dataLength = range.calculateLength();
            _contentLength = _dataLength;
        }
    }

    public boolean hasFile() { return false; }
    public boolean inline() { return true; }
    
    @Override
    public long getLength()
    {
        return _contentLength;
    }

    @Override
    public void writeContent(final OutputStream out) throws IOException
    {
        final long start = (_diagnostics == null) ? 0L : _timeMaster.nanosForDiagnostics();
        try {
            if (_dataOffset <= 0L) {
                _data.writeBytes(out);
            } else { // casts are safe; inlined data relatively small
                _data.writeBytes(out, (int) _dataOffset, (int) _dataLength);
            }
        } finally {
            if (_diagnostics != null) {
                _diagnostics.addResponseWriteTime(start, _timeMaster.nanosForDiagnostics());
            }
        }
    }
}
