package com.fasterxml.clustermate.service.msg;

import java.io.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ning.compress.lzf.LZFInputStream;

import com.fasterxml.storemate.shared.ByteContainer;
import com.fasterxml.storemate.shared.ByteRange;
import com.fasterxml.storemate.shared.compress.Compression;
import com.fasterxml.storemate.shared.compress.Compressors;
import com.fasterxml.storemate.shared.util.BufferRecycler;

/**
 * Simple (but not naive) {@link StreamingResponseContent} implementation used
 * for returning content for inlined entries.
 */
public class StreamingResponseContentImpl
    implements StreamingResponseContent
{
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    /**
     * We can reuse read buffers as they are somewhat costly to
     * allocate, reallocate all the time.
     */
    final protected static BufferRecycler _bufferRecycler = new BufferRecycler(8000);

    private final File _file;

    private final Compression _compression;
	
    private final ByteContainer _data;
	
    private final long _dataOffset;
	
    private final long _dataLength;

    public StreamingResponseContentImpl(ByteContainer data, ByteRange range)
    {
        if (data == null) {
            throw new IllegalArgumentException();
        }
        _data = data;
        // Range request? let's tweak offsets if so...
        if (range != null) {
            _dataOffset = range.getStart();
            _dataLength = range.calculateLength();
        } else {
        	_dataOffset = -1L;
        	_dataLength = -1L;
        }
        _file = null;
        _compression = null;
    }

    public StreamingResponseContentImpl(File f, Compression comp, ByteRange range)
    {
        _data = null;
        if (range == null) {
            _dataOffset = -1L;
            _dataLength = -1L;
        } else {
            // Range can be stored in offset..
            _dataOffset = range.getStart();
            _dataLength = range.calculateLength();
        }
        _file = f;
        _compression = comp;
    }

    @Override
    public long getLength()
    {
        // we may or may not know the length: for pre-loaded data we do know:
        if (_data != null) {
            return _dataLength;
        }
        // otherwise... ah, let's not worry about figuring it out, whatwith compression,
        // Ranges and such complications:
        return -1L;
    }

    @Override
    public void writeContent(OutputStream out) throws IOException
    {
        /* Inline data is simple, because we have already decompressed it
         * if and as necessary; so all we do is just write is out.
         */
        if (_data != null) {
            if (_dataOffset <= 0L) {
                _data.writeBytes(out);
            } else { // casts are safe; inlined data relatively small
                _data.writeBytes(out, (int) _dataOffset, (int) _dataLength);
            }
            return;
        }
        InputStream in = new FileInputStream(_file);
        // First: LZF has special optimization to use, if we are to copy the whole thing:
        if ((_compression == Compression.LZF) && (_dataLength == -1)) {
    	        LZFInputStream lzfIn = new LZFInputStream(in);
    	        try {
    	            lzfIn.readAndWrite(out);
    	        } finally {
    	            try {
    	                lzfIn.close();
    	            } catch (IOException e) { }
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
            try {
                in.close();
            } catch (IOException e) { }
        }
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Methods for helping testing
    ///////////////////////////////////////////////////////////////////////
     */

    public boolean hasFile() { return _file != null; }
    public boolean inline() { return _data != null; }
}
