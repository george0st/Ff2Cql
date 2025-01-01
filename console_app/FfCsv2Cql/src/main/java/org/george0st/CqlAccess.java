package org.george0st;

import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import org.george0st.codec.*;
import org.george0st.helper.Setup;

import java.net.InetSocketAddress;

public class CqlAccess {

    protected Setup setup;
    protected CqlSessionBuilder sessionBuilder;

    public CqlAccess(Setup setup) {
        this.setup = setup;
        this.sessionBuilder = createBuilder();
    }

    public CqlAccess(CqlAccess access) {
        this.setup = access.setup;
        this.sessionBuilder = access.sessionBuilder;
    }

    private CqlSessionBuilder createBuilder(){
        CqlSessionBuilder builder = new CqlSessionBuilder();

        // IP addresses
        for (String ipAddress : this.setup.ipAddresses)
            builder.addContactPoint(new InetSocketAddress(ipAddress.strip(), setup.port));

        // data center
        builder.withLocalDatacenter(setup.localDC);

        // authorization
        builder.withAuthCredentials(setup.username, setup.getPwd());

        // add codecs
        builder.addTypeCodecs(new CqlIntToStringCodec(),
                new CqlBigIntToStringCodec(),
                new CqlFloatToStringCodec(),
                new CqlDoubleToStringCodec(),
                new CqlDateToStringCodec(),
                new CqlTimeToStringCodec(),
                new CqlTimestampToStringCodec(),
                new CqlBooleanToStringCodec(),
                new CqlUUIDToStringCodec(),
                new CqlSmallintToStringCodec(),
                new CqlTinyintToStringCodec(),
                new CqlTimeUUIDToStringCodec());

        // default options (balancing, timeout, CL)
        OptionsMap options = OptionsMap.driverDefaults();
        options.put(TypedDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, setup.localDC);
        options.put(TypedDriverOption.CONNECTION_CONNECT_TIMEOUT, java.time.Duration.ofSeconds(setup.connectionTimeout));
        options.put(TypedDriverOption.REQUEST_TIMEOUT, java.time.Duration.ofSeconds(setup.requestTimeout));
        options.put(TypedDriverOption.REQUEST_CONSISTENCY, setup.consistencyLevel);
//        options.put(TypedDriverOption.PROTOCOL_COMPRESSION, "LZ4");
//        options.put(TypedDriverOption.PROTOCOL_COMPRESSION, "SNAPPY");
        options.put(TypedDriverOption.PROTOCOL_VERSION, "V4");
        builder.withConfigLoader(DriverConfigLoader.fromMap(options));

        return builder;
    }

}
