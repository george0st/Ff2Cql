package org.george0st;


import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.opencsv.exceptions.CsvValidationException;
import org.george0st.helper.Setup;
import org.george0st.processor.CsvCqlWrite;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class Main {

    public static void main(String[] args) throws CsvValidationException, IOException {


        byte[] data = new byte[]{0, 0, 1, -109, -7, -58, 26, -112};
        ByteBuffer bytes = ByteBuffer.wrap(data);

        LocalDateTime aa=LocalDateTime.ofInstant(TypeCodecs.TIMESTAMP.decode(bytes, ProtocolVersion.V4),
                ZoneId.of("Europe/London"));
        //LocalDateTime aa= LocalDateTime.from(TypeCodecs.TIMESTAMP.decode(bytes, ProtocolVersion.V4));



//        LocalDateTime datetimeValue = LocalDateTime.from(TypeCodecs.TIMESTAMP.decode(bytes, protocolVersion));
//        String value="2024-12-24T17:45:30";
//        LocalDateTime datetimeValue = LocalDateTime.parse(value);
//        Instant bb= datetimeValue.atZone(ZoneId.of("Europe/London")).toInstant();
//        ByteBuffer buff=TypeCodecs.TIMESTAMP.encode(bb, ProtocolVersion.V4);
//
//        value="2024-12-24T17:45:31";
//        datetimeValue = LocalDateTime.parse(value);
//        bb= datetimeValue.atZone(ZoneId.of("Europe/London")).toInstant();
//        ByteBuffer buff2=TypeCodecs.TIMESTAMP.encode(bb, ProtocolVersion.V4);


//        ZonedDateTime zdt;
//
//        zdt =  datetimeValue.atZone(ZoneId.of("Europe/London"));
//        Instant aaa= Instant.from(zdt.toLocalDateTime());

        //return TypeCodecs.TIMESTAMP.encode(Instant.from(zdt.toLocalDateTime()), protocolVersion);


//        String setupFile= Setup.getSetupFile(new String[]{"connection-private.json","connection.json"});
//        CsvCqlWrite aa = new CsvCqlWrite(Setup.getInstance(setupFile));
//        aa.execute("test.csv");

        System.out.printf("! DONE !");
    }
}