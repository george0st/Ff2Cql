package org.george0st.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

import java.nio.ByteBuffer;
import java.util.UUID;

//  https://docs.datastax.com/en/developer/java-driver/4.17/manual/core/custom_codecs/index.html
public class CqlUUIDToStringCodec implements TypeCodec<String> {

    @Override
    public GenericType<String> getJavaType() {
        return GenericType.STRING;
    }

    @Override
    public DataType getCqlType() {
        return DataTypes.UUID;
    }

    @Override
    public ByteBuffer encode(String value, ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        } else {
            UUID uuidValue = UUID.fromString(value);
            return TypeCodecs.UUID.encode(uuidValue, protocolVersion);
        }
    }

    @Override
    public String decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        UUID uuidValue = TypeCodecs.UUID.decode(bytes, protocolVersion);
        return uuidValue.toString();
    }

    @Override
    public String format(String value) {
        UUID uuidValue = UUID.fromString(value);
        return TypeCodecs.UUID.format(uuidValue);
    }

    @Override
    public String parse(String value) {
        UUID uuidValue = TypeCodecs.UUID.parse(value);
        return uuidValue == null ? null : uuidValue.toString();
    }
}

