package org.george0st.cql;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import org.george0st.cql.codec.*;
import org.george0st.cql.helper.ControllerSetup;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.nio.file.Paths;

/**
 * Access to the CQL source/engine with default setting such as
 * IP addresses, authorization, codecs, timeouts, consistency level, etc.).
 */
public class CQLAccess implements AutoCloseable {

    protected ControllerSetup controllerSetup;
    protected CqlSessionBuilder sessionBuilder;

    public CQLAccess(ControllerSetup controllerSetup) {
        this.controllerSetup = controllerSetup;
        this.sessionBuilder = createBuilder();
    }

    @Override
    public void close(){
        sessionBuilder = null;
        controllerSetup = null;
    }

    private CqlSessionBuilder createBuilder(){
        CqlSessionBuilder builder = new CqlSessionBuilder();

        // IP addresses (optional because secureConnectionBundle)
        if (this.controllerSetup.ipAddresses!=null)
            for (String ipAddress : this.controllerSetup.ipAddresses)
                builder.addContactPoint(new InetSocketAddress(ipAddress.strip(), controllerSetup.port));
        
        //  secureConnectionBundle
        if (controllerSetup.secureConnectionBundle!=null)
            builder.withCloudSecureConnectBundle(Paths.get(controllerSetup.secureConnectionBundle));

        // basic authorization
        if (controllerSetup.username!=null)
            builder.withAuthCredentials(controllerSetup.username, controllerSetup.pwd);

        // SSL context
        if (controllerSetup.sslContext != null)
            builder.withSslContext((SSLContext) controllerSetup.sslContext);

        // data center
        if (controllerSetup.localDC!=null)
            builder.withLocalDatacenter(controllerSetup.localDC);

        // add supported codecs
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
        if (controllerSetup.localDC!=null)
            options.put(TypedDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, controllerSetup.localDC);
        options.put(TypedDriverOption.CONNECTION_CONNECT_TIMEOUT, java.time.Duration.ofSeconds(controllerSetup.connectionTimeout));
        options.put(TypedDriverOption.REQUEST_TIMEOUT, java.time.Duration.ofSeconds(controllerSetup.requestTimeout));
        options.put(TypedDriverOption.REQUEST_CONSISTENCY, controllerSetup.consistencyLevel);
//        options.put(TypedDriverOption.PROTOCOL_COMPRESSION, "LZ4");
//        options.put(TypedDriverOption.PROTOCOL_COMPRESSION, "SNAPPY");
        options.put(TypedDriverOption.PROTOCOL_VERSION, "V4");
        builder.withConfigLoader(DriverConfigLoader.fromMap(options));

        return builder;
    }

}
