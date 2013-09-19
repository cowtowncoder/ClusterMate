package com.fasterxml.clustermate.service.metrics;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.fasterxml.jackson.core.*;

public final class SerializedMetrics
{
    public final long created;

    private final JsonFactory jsonFactory;
    
    private final byte[] serialized;

    private volatile byte[] indented;
    
    public SerializedMetrics(JsonFactory jf, long cr, byte[] data) {
        jsonFactory = jf;
        created = cr;
        serialized = data;
    }

    public byte[] getRaw() {
        return serialized;
    }

    public byte[] getIndented() throws IOException
    {
        byte[] json = indented;
        if (json == null) {
            indented = json = _indent(serialized);
        }
        return json;
    }

    private byte[] _indent(byte[] rawJson) throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream((rawJson.length * 3) / 2);
        JsonParser jp = jsonFactory.createParser(rawJson);
        JsonGenerator jg = jsonFactory.createGenerator(out);
        jg.useDefaultPrettyPrinter();
        while (jp.nextToken() != null) {
            jg.copyCurrentEvent(jp);
        }
        jp.close();
        jg.close();
        return out.toByteArray();
    }
}