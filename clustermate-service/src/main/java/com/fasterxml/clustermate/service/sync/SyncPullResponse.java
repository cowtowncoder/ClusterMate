package com.fasterxml.clustermate.service.sync;

import java.io.*;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectWriter;

import com.fasterxml.storemate.shared.EntryKey;
import com.fasterxml.storemate.shared.util.*;
import com.fasterxml.storemate.store.file.FileManager;

import com.fasterxml.clustermate.service.msg.StreamingResponseContent;
import com.fasterxml.clustermate.service.store.StoredEntry;

/**
 * Helper class used for producing response for "sync pull" requests.
 *<p>
 * Serialization of responses is slightly tricky, because the serialization
 * we want has to differ from usual 'simple' JSON or Smile structure.
 * Instead of serializing a full structure, we serialize a sequence of
 * metadata-payload pairs; both of which are preceded by 4-byte length
 * indicators; and then a 0-length marker at the end.
 */
public class SyncPullResponse<E extends StoredEntry<? extends EntryKey>>
    implements StreamingResponseContent
{
    // // We will use a crude additional verification, by surrounding
    // // 2-byte length with sentinel bytes
    
    private final static byte LENGTH_HEADER_BYTE = (byte) 0xFE;
    private final static byte LENGTH_TRAILER_BYTE = (byte) 0xFD;

    /**
     * When it rains it pours: errors love company. So to reduce noise during
     * shitstorms let's only print up to N errors per round.
     */
    protected final static int MAX_ERRORS_PER_PULL = 3;
    
    // will use 16k recyclable read buffers
    private final static int BUFFER_LENGTH = 16000;
    
    private final static Logger LOG = LoggerFactory.getLogger(SyncPullResponse.class);

    // and here's how recycling will work
    protected final static BufferRecycler _readBuffers = new BufferRecycler(BUFFER_LENGTH);

    private final FileManager _fileManager;
    
    /**
     * Smile serializer to use for metadata entries
     */
    private final ObjectWriter _smileWriter;
    
    private List<E> _entries;
    
    public SyncPullResponse(FileManager fileManager, ObjectWriter smileWriter,
            List<E> entries)
    {
        _fileManager = fileManager;
        _smileWriter = smileWriter;
        _entries = entries;
    }

    @Override
    public long getLength() {
    	// no, we do not really know the length
    	return -1L;
    }
    
    @Override
    public void writeContent(final OutputStream output) throws IOException
    {
        final int count = _entries.size();
        int warningsPrinted = 0;
        try {
            for (int i = 0; i < count; ++i) {
                E entry  = _entries.get(i);

                // Can entry actually be null? Seems unlikely, may occur since expiration
                // and clean up threads are asynchronous; so let's allow it:
                if (entry == null) {
                    LOG.warn("Missing entry ({}/{}), returned to caller as empty", i, count);
                    _writeLength(output, 0);
                    continue;
                }
                SyncPullEntry header = new SyncPullEntry(entry);
                byte[] metadata = _smileWriter.writeValueAsBytes(header);
                if (metadata.length > SyncHandler.MAX_HEADER_LENGTH) { // sanity check; never to occur...
                    LOG.error("Internal error: too long header ({}) (entry key '{}'); must skip",
                            metadata.length, entry.getKey());
                    continue;
                }
                // first: if state is DELETED, may need special handling? Or just skip...
                if (entry.isDeleted()) {
                    _writeLength(output, metadata.length);
                    output.write(metadata);
                    continue;
                }
                String warning;
                if (entry.hasExternalData()) {
                    warning = _writeExternal(output, entry, metadata);
                } else {
                    warning = _writeInlined(output, entry, metadata);
                }
                if (warning == null) { // ok, no problem
                    continue;
                }

                if (++warningsPrinted <= MAX_ERRORS_PER_PULL) {
                    if (warningsPrinted < MAX_ERRORS_PER_PULL) {
                        LOG.error("Failed to include pull entry {}/{}, reason: {}", i, count, warning);
                        /*
long age = System.currentTimeMillis() - entry.getLastModifiedTime();
long ttl = 1000L * entry.getMaxTTLSecs();
if (1 == warningsPrinted) LOG.error("AGE of failed entry (raw mod time "+entry.getLastModifiedTime()+"): "+TimeMaster.timeDesc(age)
        +", max TTL "+TimeMaster.timeDesc(ttl));
                        
                        */
                    } else {
                        LOG.error("Failed to include pull entry {}/{}, reason: {} -- NOTE: max warnings ({}) reached, will suppress rest"
                                , i, count, warning, MAX_ERRORS_PER_PULL);
                    }
                }
            }
            // and finally, write end marker
            _writeLength(output, SyncHandler.LENGTH_EOF);
            output.flush();
        } catch (FileNotFoundException e) {
        } catch (IOException e) {
            LOG.error("I/O problem during writing of "+count+" sync-pull entries: "+e.getMessage(), e);
            throw e;
        } catch (RuntimeException e) {
            LOG.error("Internal error during writing of "+count+" sync-pull entries: "+e.getMessage(), e);
            throw e;
        }
    }

    private String _writeExternal(final OutputStream output, E entry, byte[] metadata)
        throws IOException
    {
        File f = entry.getRaw().getExternalFile(_fileManager);
        FileInputStream in;
        try {
            in = new FileInputStream(f);
        } catch (FileNotFoundException e) {
            /* 15-Jan-2012, tatu: It is possible (rarely, but still) that cleanup process could
             *   delete data file before its metadata, and although sync list should try to
             *   suppress these, some may sneak. So let's take preventive action here so
             *   we won't have poison pills.
             */
            // We must still add something: "null" will have to do as indicator for missing entry
            _writeLength(output, 0);
            return "Missing file '"+f.getAbsolutePath()+"'";
        }
        _writeLength(output, metadata.length);
        output.write(metadata);
        _copyFile(f, in, output, entry.getStorageLength());
        return null;
    }

    private String _writeInlined(final OutputStream output, E entry, byte[] metadata)
        throws IOException
    {
        _writeLength(output, metadata.length);
        output.write(metadata);
        IOException e = entry.getRaw().withInlinedData(new WithBytesCallback<IOException>() {
            @Override
            public IOException withBytes(byte[] b, int offset, int length) {
                try {
                    output.write(b, offset, length);
                } catch (IOException e2) {
                    return e2;
                }
                return null;
            }
        });
        if (e != null) {
            throw e;
        }
        return null;
    }
    
    private void _copyFile(File f, FileInputStream in, OutputStream out, final long size)
        throws IOException
    {
        BufferRecycler.Holder bufferHolder = _readBuffers.getHolder();        
        final byte[] buffer = bufferHolder.borrowBuffer(BUFFER_LENGTH);

        long copied = 0;

        try {
            int count;
            while (copied < size && (count = in.read(buffer)) >= 0) {
                out.write(buffer, 0, count);
                copied += count;
            }
        } finally {
            bufferHolder.returnBuffer(buffer);
            try {
                in.close();
            } catch (IOException e) { }
        }
        // Sanity check...
        if (copied != size) {
            throw new IOException("Invalid File '"+f.getAbsolutePath()+"': should have copied "+size
                    +" bytes, instead copied "+copied);
        }
    }

    private void _writeLength(OutputStream out, int length) throws IOException
    {
        final byte[] LENGTH_BUFFER = new byte[4];
        LENGTH_BUFFER[0] = (byte) LENGTH_HEADER_BYTE;
        LENGTH_BUFFER[1] = (byte) (length >> 8);
        LENGTH_BUFFER[2] = (byte) length;
        LENGTH_BUFFER[3] = (byte) LENGTH_TRAILER_BYTE;
        out.write(LENGTH_BUFFER, 0, 4);
    }

    /**
     * @return Length of header read, if valid, or negative value if not
     */
    public static int readHeaderLength(InputStream in) throws IOException
    {
        final byte[] buf = new byte[4];
        int length = IOUtil.readFully(in, buf);
        if (length < 4) {
            throw new IOException("Unexpected end-of-stream when trying to read entry length (got "+length+"/6 bytes)");
        }
        if (buf[0] != LENGTH_HEADER_BYTE) {
            throw new IOException("Invalid length start-marker: 0x"+Integer.toHexString(buf[0] & 0xFF));
        }
        if (buf[3] != LENGTH_TRAILER_BYTE) {
            throw new IOException("Invalid length end-marker: 0x"+Integer.toHexString(buf[5] & 0xFF));
        }
        int value = ((buf[1] & 0xFF) << 8) + (buf[2] & 0xFF);
        return (int) value;
    }
}
