package org.george0st.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;

//  https://docs.datastax.com/en/developer/java-driver/4.17/manual/core/custom_codecs/index.html
public class CqlTimestampToStringCodec implements TypeCodec<String> {

    private static ZoneId zone=ZoneId.of("Europe/London");

    @Override
    public GenericType<String> getJavaType() {
        return GenericType.STRING;
    }

    @Override
    public DataType getCqlType() {
        return DataTypes.TIMESTAMP;
    }

    @Override
    public ByteBuffer encode(String value, ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        } else {
            Instant instantValue = Instant.parse(value);
            return TypeCodecs.TIMESTAMP.encode(instantValue, protocolVersion);
        }
    }

    @Override
    public String decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        Instant instantValue = TypeCodecs.TIMESTAMP.decode(bytes, protocolVersion);
        return instantValue.toString();
    }

    @Override
    public String format(String value) {
        Instant instantValue = Instant.parse(value);
        return TypeCodecs.TIMESTAMP.format(instantValue);
    }

    @Override
    public String parse(String value) {
        Instant instantValue = TypeCodecs.TIMESTAMP.parse(value);
        return instantValue == null ? null : instantValue.toString();
    }
}

