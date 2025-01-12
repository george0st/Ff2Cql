package org.george0st.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

import java.nio.ByteBuffer;

//  https://docs.datastax.com/en/developer/java-driver/4.17/manual/core/custom_codecs/index.html
public class CqlSmallintToStringCodec implements TypeCodec<String> {

    @Override
    public GenericType<String> getJavaType() {
        return GenericType.STRING;
    }

    @Override
    public DataType getCqlType() {
        return DataTypes.SMALLINT;
    }

    @Override
    public ByteBuffer encode(String value, ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        } else {
            short shortValue = Short.parseShort(value);
            return TypeCodecs.SMALLINT.encode(shortValue, protocolVersion);
        }
    }

    @Override
    public String decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        Short shortValue = TypeCodecs.SMALLINT.decode(bytes, protocolVersion);
        return shortValue.toString();
    }

    @Override
    public String format(String value) {
        short shortValue = Short.parseShort(value);
        return TypeCodecs.SMALLINT.format(shortValue);
    }

    @Override
    public String parse(String value) {
        Short shortValue = TypeCodecs.SMALLINT.parse(value);
        return shortValue == null ? null : shortValue.toString();
    }
}

