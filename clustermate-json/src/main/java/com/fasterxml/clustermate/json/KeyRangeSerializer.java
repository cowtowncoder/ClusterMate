package com.fasterxml.clustermate.json;

import java.io.IOException;

import com.fasterxml.clustermate.api.KeyRange;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;


/**
 * We don't absolutely need a serializer for this type, but it's
 * simple enough to do so.
 */
public class KeyRangeSerializer extends StdSerializer<KeyRange>
{
	private static final long serialVersionUID = 1L;

	public KeyRangeSerializer() { super(KeyRange.class); }

    @Override
    public void serialize(KeyRange value, JsonGenerator jgen, SerializerProvider provider)
        throws IOException, JsonGenerationException
    {
        jgen.writeStartObject();
        _serialize(value, jgen, provider);
        jgen.writeEndObject();
    }

    @Override
    public void serializeWithType(KeyRange value, JsonGenerator jgen,
            SerializerProvider provider, TypeSerializer typeSer)
                    throws IOException, JsonProcessingException
    {
        typeSer.writeTypePrefixForObject(value, jgen);
        _serialize(value, jgen, provider);
        typeSer.writeTypeSuffixForObject(value, jgen);
    }

    protected void _serialize(KeyRange value, JsonGenerator jgen, SerializerProvider provider)
        throws IOException, JsonGenerationException
    {
        jgen.writeNumberField("keyspace", value.getKeyspace().getLength());
        jgen.writeNumberField("start", value.getStart());
        jgen.writeNumberField("length", value.getLength());
    }
}
