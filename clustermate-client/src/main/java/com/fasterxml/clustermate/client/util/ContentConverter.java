package com.fasterxml.clustermate.client.util;

import java.io.*;

import com.fasterxml.clustermate.api.ContentType;

/**
 * Base class for objects that are used for converting entities, by
 * decoding and/or parsing entities from stream or chunked input.
 */
public interface ContentConverter<T>
{
    public T convert(ContentType contentType, InputStream in) throws IOException;

    public T convert(ContentType contentType, byte[] buffer, int offset, int length) throws IOException;
}
