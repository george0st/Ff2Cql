package org.george0st.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import java.nio.ByteBuffer;
import java.time.LocalTime;


//  https://docs.datastax.com/en/developer/java-driver/4.17/manual/core/custom_codecs/index.html
public class CqlTimeToStringCodec implements TypeCodec<String> {

    @Override
    public GenericType<String> getJavaType() {
        return GenericType.STRING;
    }

    @Override
    public DataType getCqlType() {
        return DataTypes.TIME;
    }

    @Override
    public ByteBuffer encode(String value, ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        } else {
            LocalTime timeValue = LocalTime.parse(value);
            return TypeCodecs.TIME.encode(timeValue, protocolVersion);
        }
    }

    @Override
    public String decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        LocalTime timeValue = TypeCodecs.TIME.decode(bytes, protocolVersion);
        return timeValue.toString();
    }

    @Override
    public String format(String value) {
        LocalTime timeValue = LocalTime.parse(value);
        return TypeCodecs.TIME.format(timeValue);
    }

    @Override
    public String parse(String value) {
        LocalTime timeValue = TypeCodecs.TIME.parse(value);
        return timeValue == null ? null : timeValue.toString();
    }
}

