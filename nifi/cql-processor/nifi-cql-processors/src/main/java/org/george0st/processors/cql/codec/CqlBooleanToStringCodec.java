package org.george0st.processors.cql.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import java.nio.ByteBuffer;

//  https://docs.datastax.com/en/developer/java-driver/4.17/manual/core/custom_codecs/index.html
public class CqlBooleanToStringCodec implements TypeCodec<String> {

    @Override
    public GenericType<String> getJavaType() {
        return GenericType.STRING;
    }

    @Override
    public DataType getCqlType() {
        return DataTypes.BOOLEAN;
    }

    @Override
    public ByteBuffer encode(String value, ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        } else {
            boolean booleanValue = Boolean.parseBoolean(value);
            return TypeCodecs.BOOLEAN.encode(booleanValue, protocolVersion);
        }
    }

    @Override
    public String decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        Boolean booleanValue = TypeCodecs.BOOLEAN.decode(bytes, protocolVersion);
        return booleanValue.toString();
    }

    @Override
    public String format(String value) {
        boolean booleanValue = Boolean.parseBoolean(value);
        return TypeCodecs.BOOLEAN.format(booleanValue);
    }

    @Override
    public String parse(String value) {
        Boolean booleanValue = TypeCodecs.BOOLEAN.parse(value);
        return booleanValue == null ? null : booleanValue.toString();
    }
}

