package org.george0st.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

import java.nio.ByteBuffer;
import java.time.*;

//  https://docs.datastax.com/en/developer/java-driver/4.17/manual/core/custom_codecs/index.html
public class CqlTimestampToStringCodec implements TypeCodec<String> {

    private ZoneId zone=ZoneId.of("Europe/London");

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
            //https://stackoverflow.com/questions/77017326/unable-to-obtain-instant-from-temporalaccessor-2023-08-31t203749-005832800-of

            LocalDateTime datetimeValue = LocalDateTime.parse(value);
            return TypeCodecs.TIMESTAMP.encode(datetimeValue.atZone(zone).toInstant(),
                    protocolVersion);
            //return TypeCodecs.TIMESTAMP.encode(Instant.from(datetimeValue), protocolVersion);
        }
    }

    @Override
    public String decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        LocalDateTime datetimeValue=LocalDateTime.ofInstant(TypeCodecs.TIMESTAMP.decode(bytes, protocolVersion), zone);
        return datetimeValue.toString();
    }

    @Override
    public String format(String value) {
        LocalDateTime datetimeValue = LocalDateTime.parse(value);
        return TypeCodecs.TIMESTAMP.format(datetimeValue.atZone(zone).toInstant());
    }

    @Override
    public String parse(String value) {
        //LocalDateTime dateTimeValue = LocalDateTime.from(TypeCodecs.TIMESTAMP.parse(value));
        LocalDateTime dateTimeValue = LocalDateTime.from(TypeCodecs.TIMESTAMP.parse(value).atZone(zone));
        return dateTimeValue == null ? null : dateTimeValue.toString();
    }
}

