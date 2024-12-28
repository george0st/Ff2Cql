package org.george0st.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import java.nio.ByteBuffer;
import java.time.LocalDate;


//  https://docs.datastax.com/en/developer/java-driver/4.17/manual/core/custom_codecs/index.html
public class CqlDateToStringCodec implements TypeCodec<String> {

    @Override
    public GenericType<String> getJavaType() {
        return GenericType.STRING;
    }

    @Override
    public DataType getCqlType() {
        return DataTypes.DATE;
    }

    @Override
    public ByteBuffer encode(String value, ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        } else {
            LocalDate dateValue = LocalDate.parse(value);
            return TypeCodecs.DATE.encode(dateValue, protocolVersion);
        }
    }

    @Override
    public String decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        LocalDate dateValue = TypeCodecs.DATE.decode(bytes, protocolVersion);
        return dateValue.toString();
    }

    @Override
    public String format(String value) {
        LocalDate dateValue = LocalDate.parse(value);
        return TypeCodecs.DATE.format(dateValue);
    }

    @Override
    public String parse(String value) {
        LocalDate dateValue = TypeCodecs.DATE.parse(value);
        return dateValue == null ? null : dateValue.toString();
    }
}

