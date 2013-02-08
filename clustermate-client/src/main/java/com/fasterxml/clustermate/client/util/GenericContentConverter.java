package com.fasterxml.clustermate.client.util;

import java.io.*;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;

import com.fasterxml.clustermate.api.ContentType;

/**
 * {link ContentConverter} implementation that handles variations
 * in two dimensions: in target type (one converter instance created
 * per type), and in underlying data format (which is chosen dynamically)
 */
public class GenericContentConverter<T> implements ContentConverter<T>
{
    protected final static SmileFactory SMILE_FACTORY = new SmileFactory();

    protected final ObjectReader _jsonReader;

    public GenericContentConverter(ObjectMapper jsonMapper, Class<T> targetType) {
        _jsonReader = jsonMapper.reader(targetType);
    }

    public GenericContentConverter(ObjectMapper jsonMapper, JavaType targetType) {
        _jsonReader = jsonMapper.reader(targetType);
    }
    
    @Override
    public T convert(ContentType contentType, InputStream in)
        throws IOException
    {
        return _chooseReader(contentType).readValue(in);
    }

    @Override
    public T convert(ContentType contentType, byte[] buffer, int offset,
            int length) throws IOException {
        return _chooseReader(contentType).readValue(buffer, offset, length);
    }

    protected ObjectReader _chooseReader(ContentType contentType) {
        switch (contentType) {
        case SMILE: // only special case is Smile
            return _jsonReader.with(SMILE_FACTORY);
        case JSON:
            return _jsonReader;
        default:
        }
        // TEXT not supported..
        throw new IllegalArgumentException("Can not handle content type '"+contentType+"'");
    }
}
