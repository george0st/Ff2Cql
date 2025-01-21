package org.george0st.processors.cql.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

import java.nio.ByteBuffer;

//  https://docs.datastax.com/en/developer/java-driver/4.17/manual/core/custom_codecs/index.html
public class CqlBigIntToStringCodec implements TypeCodec<String> {

    @Override
    public GenericType<String> getJavaType() {
        return GenericType.STRING;
    }

    @Override
    public DataType getCqlType() {
        return DataTypes.BIGINT;
    }

    @Override
    public ByteBuffer encode(String value, ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        } else {
            long longValue = Long.parseLong(value);
            return TypeCodecs.BIGINT.encode(longValue, protocolVersion);
        }
    }

    @Override
    public String decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        Long longValue = TypeCodecs.BIGINT.decode(bytes, protocolVersion);
        return longValue.toString();
    }

    @Override
    public String format(String value) {
        long longValue = Long.parseLong(value);
        return TypeCodecs.BIGINT.format(longValue);
    }

    @Override
    public String parse(String value) {
        Long longValue = TypeCodecs.BIGINT.parse(value);
        return longValue == null ? null : longValue.toString();
    }
}

