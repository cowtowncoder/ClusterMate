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
 * {@link StreamingResponseContent} implementation used
 * for returning content for entries that have external File-backed
 * payload.
 * Difference to {@link SimpleStreamingResponseContent} is that
 * due to file system access, more care has to be taken, including
 * allowing possible throttling of reading of content to output.
 */
public class FileBackedResponseContentImpl
    implements StreamingResponseContent
{
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    /**
     * Let's use large enough read buffer to allow read-all-write-all
     * cases.
     */
    private final static int READ_BUFFER_LENGTH = 64000;
    
    /*
    /**********************************************************************
    /* Helper objects
    /**********************************************************************
     */
    
    /**
     * We can reuse read buffers as they are somewhat costly to
     * allocate, reallocate all the time.
     */
    final protected static BufferRecycler _bufferRecycler = new BufferRecycler(READ_BUFFER_LENGTH);

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
            _dataLength = contentLen;
        } else {
            // Range can be stored in offset..
            _dataOffset = range.getStart();
            _dataLength = range.calculateLength();
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
        return _dataLength;
    }

    /*
    /**********************************************************************
    /* Actual streaming
    /**********************************************************************
     */
    
    @Override
    public void writeContent(final OutputStream out) throws IOException
    {
        final BufferRecycler.Holder bufferHolder = _bufferRecycler.getHolder();        
        final byte[] copyBuffer = bufferHolder.borrowBuffer();
        try {
            // 4 main combinations: compressed/not-compressed, range/no-range
            // and then 2 variations; fits in buffer or not
    
            // Start with uncompressed
            if (!Compression.needsUncompress(_compression)) {
                // and if all we need fits in the buffer, read all, write all:
                if (_dataLength <= READ_BUFFER_LENGTH) {
                    _readAllWriteAllUncompressed(out, copyBuffer, _dataOffset, (int) _dataLength);
                } else {
                    // if not, need longer lock...
                    _readAllWriteStreamingUncompressed(out, copyBuffer, _dataOffset, _dataLength);
                }
                return;
            }
            
            // And then compressed variants
            
        } finally {
            bufferHolder.returnBuffer(copyBuffer);
        }

        // If not, use (for now) the old slow read-uncompress-copy loop:
        
        _throttler.performFileRead(new FileOperationCallback<Void>() {
            @Override
            public Void perform(long operationTime, Storable value, File externalFile)
                    throws IOException, StoreException
            {
                _writeContentFromFile(out);
                return null;
            }
        }, _operationTime, _entry.getRaw(), _file);
    }

    /*
    /**********************************************************************
    /* Second level copy methods; uncompressed data
    /**********************************************************************
     */

    /**
     * Method called for the simple case where we can just read all data into
     * single buffer (and do that in throttled block), then write it out
     * at our leisure.
     */
    protected void _readAllWriteAllUncompressed(OutputStream out, final byte[] copyBuffer,
            final long offset, final int dataLength)
        throws IOException
    {
        int length = _throttler.performFileRead(new FileOperationCallback<Integer>() {
            @Override
            public Integer perform(long operationTime, Storable value, File externalFile)
                throws IOException
            {
                return _readFromFile(externalFile, copyBuffer, offset, dataLength);
            }
        }, _operationTime, _entry.getRaw(), _file);
        // and write out like so
        out.write(copyBuffer, 0, length);
    }

    /**
     * Method called in cases where content does not all fit in a copy buffer
     * and has to be streamed.
     */
    protected void _readAllWriteStreamingUncompressed(final OutputStream out, final byte[] copyBuffer,
            final long offset, final long dataLength)
        throws IOException
    {
        _throttler.performFileRead(new FileOperationCallback<Void>() {
            @Override
            public Void perform(long operationTime, Storable value, File externalFile)
                throws IOException
            {
                InputStream in = new FileInputStream(_file);
                try {
                    if (offset > 0L) {
                        _skip(in, offset);
                    }
                    long left = dataLength;
                    while (left >= 0L) {
                        int read = _read(in, copyBuffer, left);
                        out.write(copyBuffer, 0, read);
                        left -= read;
                    }
                } finally {
                    _close(in);
                }
                return null;
            }
        }, _operationTime, _entry.getRaw(), _file);
    }
    
    /*
    /**********************************************************************
    /* Second level copy methods; compressed data
    /**********************************************************************
     */
    
    @SuppressWarnings("resource")
    protected void _writeContentFromFile(OutputStream out) throws IOException
    {
        InputStream in = new FileInputStream(_file);
        
        // First: LZF has special optimization to use, if we are to copy the whole thing:
        if ((_compression == Compression.LZF) && (_dataOffset < 0L)) {
    	        LZFInputStream lzfIn = new LZFInputStream(in);
    	        try {
    	            lzfIn.readAndWrite(out);
    	        } finally {
    	            _close(lzfIn);
    	        }
    	        return;
        }

        // and for now, compressed variants are handle
        
        // otherwise default handling via explicit copying
        final BufferRecycler.Holder bufferHolder = _bufferRecycler.getHolder();        
        final byte[] copyBuffer = bufferHolder.borrowBuffer();

        in = Compressors.uncompressingStream(in, _compression);

        // First: anything to skip (only the case for range requests)?
        if (_dataOffset > 0L) {
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

    /*
    /**********************************************************************
    /* Simple helper methods
    /**********************************************************************
     */

    /**
     * Helper method for reading all the content from specified file
     * into given buffer. Caller must ensure that amount to read fits
     * in the buffer.
     */
    protected int _readFromFile(File f, byte[] buffer, long toSkip, int dataLength) throws IOException
    {
        InputStream in = new FileInputStream(_file);
        int offset = 0;

        try {
            // Content to skip?
            if (toSkip > 0L) {
                _skip(in, toSkip);
            }
            int left = dataLength;
            while (left > 0) {
                int count = in.read(buffer, offset, left);
                if (count <= 0) {
                    if (count == 0) {
                        throw new IOException("Weird stream ("+in+"): read for "+left+" bytes returned "+count);
                    }
                    break;
                }
                offset += count;
                left -= count;
            }
        } finally {
            _close(in);
        }
        return offset;
    }

    protected final int _read(InputStream in, byte[] buffer, long maxRead) throws IOException
    {
        int toRead = (int) Math.min(buffer.length, maxRead);
        int offset = 0;
        while (offset < toRead) {
            int count= in.read(buffer, offset, toRead-offset);
            if (count <= 0) {
                throw new IOException("Failed to read next "+toRead+" bytes (of total needed: "+maxRead+"): only got "
                        +offset);
            }
            offset += count;
        }
        return toRead;
    }
    
    protected final void _skip(InputStream in, long toSkip) throws IOException
    {
        long skipped = 0L;
        while (skipped < toSkip) {
            long count = in.skip(toSkip - skipped);
            if (count <= 0L) { // should not occur really...
                throw new IOException("Failed to skip more than "+skipped+" bytes (needed to skip "+_dataOffset+")");
            }
            skipped += count;
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
