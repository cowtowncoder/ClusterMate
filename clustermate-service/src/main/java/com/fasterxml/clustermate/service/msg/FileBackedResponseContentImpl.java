package com.fasterxml.clustermate.service.msg;

import java.io.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ning.compress.DataHandler;
import com.ning.compress.Uncompressor;
import com.ning.compress.gzip.GZIPUncompressor;
import com.ning.compress.lzf.LZFInputStream;
import com.ning.compress.lzf.LZFUncompressor;

import com.fasterxml.storemate.shared.ByteRange;
import com.fasterxml.storemate.shared.StorableKey;
import com.fasterxml.storemate.shared.TimeMaster;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.shared.compress.Compressors;
import com.fasterxml.storemate.shared.util.BufferRecycler;
import com.fasterxml.storemate.store.FileOperationCallback;
import com.fasterxml.storemate.store.Storable;
import com.fasterxml.storemate.store.StoreException;
import com.fasterxml.storemate.store.StoreOperationSource;
import com.fasterxml.storemate.store.StoreOperationThrottler;
import com.fasterxml.storemate.store.util.OperationDiagnostics;

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

    final protected OperationDiagnostics _diagnostics;
    
    final protected TimeMaster _timeMaster;

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

    public FileBackedResponseContentImpl(OperationDiagnostics diag, TimeMaster timeMaster,
            StoreOperationThrottler throttler, long operationTime,
            File f, Compression comp, ByteRange range,
            StoredEntry<?> entry)
    {
        _diagnostics = diag;
        _timeMaster = timeMaster;
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
            
            // And then compressed variants. First, maybe we can read all data in memory before uncomp?
            if (_fileLength <= READ_BUFFER_LENGTH) {
                _readAllWriteAllCompressed(out, copyBuffer, _dataOffset, _dataLength);
            } else {
                final long start = (_diagnostics == null) ? 0L : _timeMaster.nanosForDiagnostics();
                // If not, use (for now) the old slow read-uncompress-copy loop:
                _throttler.performFileRead(StoreOperationSource.REQUEST,
                        _operationTime, _entry.getRaw(), _file,
                        new FileOperationCallback<Void>() {
                    @Override
                    public Void perform(long operationTime, StorableKey key, Storable value, File externalFile)
                            throws IOException, StoreException
                    {
                        final long fsStart = (_diagnostics == null) ? 0L : _timeMaster.nanosForDiagnostics();
                        _readAllWriteStreamingCompressed(out, copyBuffer);
                        if (_diagnostics != null) {
                            _diagnostics.addFileAccess(start, fsStart, _timeMaster.nanosForDiagnostics());
                        }
                        return null;
                    }
                });
            }
        } finally {
            bufferHolder.returnBuffer(copyBuffer);
        }
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
        _readAll(out, copyBuffer, offset, dataLength);
        // and write out like so
        final long start = (_diagnostics == null) ? 0L : _timeMaster.nanosForDiagnostics();
        try {
            out.write(copyBuffer, 0, dataLength);
        } finally {
            if (_diagnostics != null) {
                _diagnostics.addResponseWriteTime(start, _timeMaster.nanosForDiagnostics());
            }
        }
    }

    /**
     * Method called in cases where content does not all fit in a copy buffer
     * and has to be streamed.
     */
    protected void _readAllWriteStreamingUncompressed(final OutputStream out, final byte[] copyBuffer,
            final long offset, final long dataLength)
        throws IOException
    {
        final long fsWaitStart = (_diagnostics == null) ? 0L : _timeMaster.nanosForDiagnostics();
        _throttler.performFileRead(StoreOperationSource.REQUEST,
                _operationTime, _entry.getRaw(), _file,
                new FileOperationCallback<Void>() {
            @Override
            public Void perform(long operationTime, StorableKey key, Storable value, File externalFile)
                throws IOException
            {
                // gets tricky, so process wait separately
                if (_diagnostics != null) {
                    _diagnostics.addFileWait( _timeMaster.nanosForDiagnostics() - fsWaitStart);
                }
                InputStream in = new FileInputStream(_file);
                try {
                    if (offset > 0L) {
                        final long start = (_diagnostics == null) ? 0L : _timeMaster.nanosForDiagnostics();
                        _skip(in, offset);
                        if (_diagnostics != null) {
                            _diagnostics.addFileAccess(start, start, _timeMaster.nanosForDiagnostics());
                        }
                    }
                    long left = dataLength;
                    while (left >= 0L) {
                        final long fsStart = (_diagnostics == null) ? 0L : _timeMaster.nanosForDiagnostics();
                        int read = _read(in, copyBuffer, left);
                        if (_diagnostics != null) {
                            _diagnostics.addFileAccess(fsStart,  fsStart, _timeMaster.nanosForDiagnostics());
                        }
                        final long writeStart = (_diagnostics == null) ? 0L : _timeMaster.nanosForDiagnostics();
                        out.write(copyBuffer, 0, read);
                        left -= read;
                        if (_diagnostics != null) {
                            _diagnostics.addResponseWriteTime(writeStart, _timeMaster.nanosForDiagnostics());
                        }
                    }
                } finally {
                    _close(in);
                }
                return null;
            }
        });
    }
    
    /*
    /**********************************************************************
    /* Second level copy methods; compressed data
    /**********************************************************************
     */

    /**
     * Method called in cases where the compressed file can be fully read
     * in a single buffer, to be uncompressed and written.
     */
    protected void _readAllWriteAllCompressed(final OutputStream out, final byte[] copyBuffer,
            final long offset, final long dataLength)
        throws IOException
    {
        // important: specify no offset, file length; data offset/length is for _uncompressed_
        int inputLength = (int) _fileLength;
        _readAll(out, copyBuffer, 0L, inputLength);

        // Compress-Ning package allows "push" style uncompression (yay!)
        Uncompressor uncomp;
        DataHandler h = new RangedDataHandler(out, offset, dataLength);

        if (_compression == Compression.LZF) {
            uncomp = new LZFUncompressor(h);
        } else if (_compression == Compression.GZIP) {
            uncomp = new GZIPUncompressor(h);
        } else { // otherwise, must use bulk operations?
            // TODO: currently we do not have other codecs
            throw new UnsupportedOperationException("No Uncompressor for compression type: "+_compression);
        }

        final long start = (_diagnostics == null) ? 0L : _timeMaster.nanosForDiagnostics();
        uncomp.feedCompressedData(copyBuffer, 0, inputLength);
        uncomp.complete();
        if (_diagnostics != null) {
            _diagnostics.addResponseWriteTime(start, _timeMaster);
        }
    }

    /**
     * Method called in the complex case of having to read a large piece of
     * content, where source does not fit in the input buffer.
     */
    @SuppressWarnings("resource")
    protected void _readAllWriteStreamingCompressed(OutputStream out, byte[] copyBuffer)
        throws IOException
    {
        InputStream in = new FileInputStream(_file);
        
        // First: LZF has special optimization to use, if we are to copy the whole thing:
        if ((_compression == Compression.LZF) && (_dataOffset < 0L)) {
            final long start = (_diagnostics == null) ? 0L : _timeMaster.nanosForDiagnostics();
    	        LZFInputStream lzfIn = new LZFInputStream(in);
    	        try {
    	            lzfIn.readAndWrite(out);
    	        } finally {
    	            _close(lzfIn);
        	        if (_diagnostics != null) {
        	            // this is not good, doubles times but...
        	            final long now = _timeMaster.nanosForDiagnostics();
        	            _diagnostics.addResponseWriteTime(start, now);
        	            _diagnostics.addFileAccess(start,  start, now);
        	        }
    	        }
    	        return;
        }

        // and for now, compressed variants are handle

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
            _close(in);
        }
    }

    /*
    /**********************************************************************
    /* Shared file access methods
    /**********************************************************************
     */

    protected void _readAll(OutputStream out, final byte[] copyBuffer,
            final long offset, final int dataLength)
        throws IOException
    {
        final long start = (_diagnostics == null) ? 0L : _timeMaster.nanosForDiagnostics();
        _throttler.performFileRead(StoreOperationSource.REQUEST,
                _operationTime, _entry.getRaw(), _file,
                new FileOperationCallback<Void>() {
            @Override
            public Void perform(long operationTime,  StorableKey key, Storable value, File externalFile)
                throws IOException
            {
                final long fsStart = (_diagnostics == null) ? 0L : _timeMaster.nanosForDiagnostics();
                int count = _readFromFile(externalFile, copyBuffer, offset, dataLength);
                if (_diagnostics != null) {
                    _diagnostics.addFileAccess(start, fsStart, _timeMaster.nanosForDiagnostics());
                }
                if (count < dataLength) {
                    throw new IOException("Failed to read all "+dataLength+" bytes from '"
                            +externalFile.getAbsolutePath()+"'; only got: "+count);
                }
                return null;
            }
        });
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

    /*
    /**********************************************************************
    /* Helper classes
    /**********************************************************************
     */
    
    /**
     * {@link DataHandler} implementation we use to extract out optional
     * ranges, writing out content as it becomes available
     */
    static class RangedDataHandler implements DataHandler
    {
        protected final OutputStream _out;
        protected final long _fullDataLength;
        protected long _leftToSkip;
        protected long _leftToWrite;

        public RangedDataHandler(OutputStream out, long offset, long dataLength)
        {
            _out = out;
            _fullDataLength = dataLength;
            _leftToSkip = offset;
            _leftToWrite = dataLength;
        }
        
        @Override
        public void handleData(byte[] buffer, int offset, int len) throws IOException
        {
            if (_leftToSkip > 0L) {
                if (len <= _leftToSkip) {
                    _leftToSkip -= len;
                    return;
                }
                offset += (int) _leftToSkip;
                len -= (int) _leftToSkip;
                _leftToSkip = 0L;
            }
            if (_leftToWrite > 0L) {
                if (len > _leftToWrite) {
                    len = (int) _leftToWrite;
                }
                _out.write(buffer, offset, len);
                _leftToWrite -= len;
            }
        }

        @Override
        public void allDataHandled() throws IOException {
            if (_leftToWrite > 0L) {
                throw new IOException("Could not uncompress all data ("+_fullDataLength+" bytes): missing last "+_leftToWrite);
            }
        }
    };
}
