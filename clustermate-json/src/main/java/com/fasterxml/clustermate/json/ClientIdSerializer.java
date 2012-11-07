package com.fasterxml.clustermate.json;

import java.io.IOException;

import com.fasterxml.clustermate.api.ClientId;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdScalarSerializer;


public class ClientIdSerializer extends StdScalarSerializer<ClientId>
{
    protected final boolean _forceNumerics;
    
    public ClientIdSerializer(boolean forceNumerics) {
        super(ClientId.class);
        _forceNumerics = forceNumerics;
    }
    
    @Override
    public void serialize(ClientId value, JsonGenerator jgen, SerializerProvider provider)
            throws IOException, JsonGenerationException
    {
        if (_forceNumerics || !value.isMnemonic()) {
            jgen.writeNumber(value.asInt());
        } else {
            jgen.writeString(value.toString());
        }
    }

}
