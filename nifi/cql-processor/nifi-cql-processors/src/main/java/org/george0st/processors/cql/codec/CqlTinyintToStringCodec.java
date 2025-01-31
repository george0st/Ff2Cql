package org.george0st.processors.cql.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import java.nio.ByteBuffer;

//  https://docs.datastax.com/en/developer/java-driver/4.17/manual/core/custom_codecs/index.html
public class CqlTinyintToStringCodec implements TypeCodec<String> {

    @Override
    public GenericType<String> getJavaType() {
        return GenericType.STRING;
    }

    @Override
    public DataType getCqlType() {
        return DataTypes.TINYINT;
    }

    @Override
    public ByteBuffer encode(String value, ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        } else {
            Byte byteValue = Byte.parseByte(value);
            return TypeCodecs.TINYINT.encode(byteValue, protocolVersion);
        }
    }

    @Override
    public String decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        Byte byteValue = TypeCodecs.TINYINT.decode(bytes, protocolVersion);
        return byteValue.toString();
    }

    @Override
    public String format(String value) {
        byte byteValue = Byte.parseByte(value);
        return TypeCodecs.TINYINT.format(byteValue);
    }

    @Override
    public String parse(String value) {
        Byte byteValue = TypeCodecs.TINYINT.parse(value);
        return byteValue == null ? null : byteValue.toString();
    }
}

