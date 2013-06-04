package com.fasterxml.clustermate.service.msg;

import java.io.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ning.compress.lzf.LZFInputStream;

import com.fasterxml.storemate.shared.ByteRange;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.shared.compress.Compressors;
import com.fasterxml.storemate.shared.util.BufferRecycler;
import com.fasterxml.storemate.store.FileOperationCallback;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.StoreException;
import com.fasterxml.storemate.store.StoreOperationThrottler;

import com.fasterxml.clustermate.service.store.StoredEntry;

/**
 * Simple (but not naive) {@link StreamingResponseContent} implementation used
 * for returning content for inlined entries.
 */
public class FileBackedResponseContentImpl
    implements StreamingResponseContent
{
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    /*
    /**********************************************************************
    /* Helper objects
    /**********************************************************************
     */
    
    /**
     * We can reuse read buffers as they are somewhat costly to
     * allocate, reallocate all the time.
     */
    final protected static BufferRecycler _bufferRecycler = new BufferRecycler(32000);

    final protected StoreOperationThrottler _throttler;
    
    /*
    /**********************************************************************
    /* Data to stream out
    /**********************************************************************
     */

    private final StoredEntry<?> _entry;

    private final File _file;
	
    private final long _dataOffset;
	
    private final long _dataLength;

    /*
    /**********************************************************************
    /* Metadata
    /**********************************************************************
     */

    private final long _operationTime;
    
    private final Compression _compression;

    /**
     * Content length as reported when caller asks for it; -1 if not known.
     */
    private final long _contentLength;

    /**
     * When reading from a file, this indicates length of content before
     * processing (if any).
     */
    private final long _fileLength;

    /*
    /**********************************************************************
    /* Construction
    /**********************************************************************
     */

    public FileBackedResponseContentImpl(StoreOperationThrottler throttler, long operationTime,
            File f, Compression comp, ByteRange range,
            StoredEntry<?> entry)
    {
        _throttler = throttler;
        _operationTime = operationTime;
        _entry = entry;
        _fileLength = entry.getStorageLength();
        long contentLen = (comp == null) ? _fileLength : entry.getActualUncompressedLength();

        if (range == null) {
            _dataOffset = -1L;
            _dataLength = -1L;
            _contentLength = contentLen;
        } else {
            // Range can be stored in offset..
            _dataOffset = range.getStart();
            _dataLength = range.calculateLength();
            _contentLength = _dataLength;
        }
        _file = f;
        _compression = comp;
    }

    /*
    /**********************************************************************
    /* Metadata
    /**********************************************************************
     */

    @Override
    public boolean hasFile() { return true; }
    @Override
    public boolean inline() { return false; }
    
    @Override
    public long getLength() {
        return _contentLength;
    }

    /*
    /**********************************************************************
    /* Actual streaming
    /**********************************************************************
     */
    
    @Override
    public void writeContent(final OutputStream out) throws IOException
    {
        _throttler.performFileRead(new FileOperationCallback() {
            @Override
            public void perform(long operationTime, Storable value, File externalFile)
                    throws IOException, StoreException
            {
                _writeContentFromFile(out);
            }
        }, _operationTime, _entry.getRaw(), _file);
    }
    
    @SuppressWarnings("resource")
    protected void _writeContentFromFile(OutputStream out) throws IOException
    {
        InputStream in = new FileInputStream(_file);
        
        // First: LZF has special optimization to use, if we are to copy the whole thing:
        if ((_compression == Compression.LZF) && (_dataLength == -1)) {
    	        LZFInputStream lzfIn = new LZFInputStream(in);
    	        try {
    	            lzfIn.readAndWrite(out);
    	        } finally {
    	            _close(lzfIn);
    	        }
    	        return;
        }

        // otherwise default handling via explicit copying
        final BufferRecycler.Holder bufferHolder = _bufferRecycler.getHolder();        
        final byte[] copyBuffer = bufferHolder.borrowBuffer();

        in = Compressors.uncompressingStream(in, _compression);

        // First: anything to skip (only the case for range requests)?
        if (_dataOffset > 0) {
            long skipped = 0L;
            long toSkip = _dataOffset;

            while (toSkip > 0) {
                long count = in.skip(toSkip);
                if (count <= 0L) { // should not occur really...
                    throw new IOException("Failed to skip more than "+skipped+" bytes (needed to skip "+_dataOffset+")");
                }
                skipped += count;
                toSkip -= count;
            }
        }
        // Second: output the whole thing, or just subset?
        try {
            if (_dataLength < 0) { // all of it
                int count;
                while ((count = in.read(copyBuffer)) > 0) {
                    out.write(copyBuffer, 0, count);
                }
                return;
            }
            // Just some of it
            long left = _dataLength;
    	    
            while (left > 0) {
                int count = in.read(copyBuffer, 0, (int) Math.min(copyBuffer.length, left));
                if (count <= 0) {
                    break;
                }
                out.write(copyBuffer, 0, count);
                left -= count;
            }
            // Sanity check; can't fix or add headers as output has been written...
            if (left > 0) {
                LOG.error("Failed to write request Range %d-%d (from File {}): only wrote {} bytes",
    	                new Object[] { _dataOffset, _dataOffset+_dataLength+1, _file.getAbsolutePath(),
    	                _dataLength-left });
            }
        } finally {
            bufferHolder.returnBuffer(copyBuffer);
            _close(in);
        }
    }

    private final void _close(InputStream in)
    {
        try {
            in.close();
        } catch (IOException e) {
            LOG.warn("Failed to close file '{}': {}", _file, e.getMessage());
        }
    }
}
