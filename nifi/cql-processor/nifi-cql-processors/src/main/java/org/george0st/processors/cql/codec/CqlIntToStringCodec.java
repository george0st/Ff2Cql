package org.george0st.processors.cql.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

import java.nio.ByteBuffer;

//  https://docs.datastax.com/en/developer/java-driver/4.17/manual/core/custom_codecs/index.html
public class CqlIntToStringCodec implements TypeCodec<String> {

    @Override
    public GenericType<String> getJavaType() {
        return GenericType.STRING;
    }

    @Override
    public DataType getCqlType() {
        return DataTypes.INT;
    }

    @Override
    public ByteBuffer encode(String value, ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        } else {
            int intValue = Integer.parseInt(value);
            return TypeCodecs.INT.encode(intValue, protocolVersion);
        }
    }

    @Override
    public String decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        Integer intValue = TypeCodecs.INT.decode(bytes, protocolVersion);
        return intValue.toString();
    }

    @Override
    public String format(String value) {
        int intValue = Integer.parseInt(value);
        return TypeCodecs.INT.format(intValue);
    }

    @Override
    public String parse(String value) {
        Integer intValue = TypeCodecs.INT.parse(value);
        return intValue == null ? null : intValue.toString();
    }
}

