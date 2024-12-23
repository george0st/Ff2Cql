package org.george0st.processor;

import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.opencsv.exceptions.CsvValidationException;
import org.george0st.codec.CqlBigIntToStringCodec;
import org.george0st.codec.CqlIntToStringCodec;
import org.george0st.helper.Setup;

import java.io.IOException;
import java.net.InetSocketAddress;

abstract class CqlProcessor {

    protected Setup setup;
    protected CqlSessionBuilder sessionBuilder;

    public CqlProcessor(Setup setup) {
        this.setup = setup;
        this.sessionBuilder = createBuilder();
    }

    private CqlSessionBuilder createBuilder(){
        CqlSessionBuilder builder = new CqlSessionBuilder();

        // IP addresses
        for (String ipAddress : this.setup.ipAddresses)
            builder.addContactPoint(new InetSocketAddress(ipAddress.strip(), setup.port));

        // data center
        builder.withLocalDatacenter(setup.localDC);

        // authorization
        builder.withAuthCredentials(setup.username, setup.pwd);

        // add codecs
        builder.addTypeCodecs(new CqlIntToStringCodec(), new CqlBigIntToStringCodec());

        // default options (balancing, timeout, CL)
        OptionsMap options = OptionsMap.driverDefaults();
        options.put(TypedDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, setup.localDC);
        options.put(TypedDriverOption.CONNECTION_CONNECT_TIMEOUT, java.time.Duration.ofSeconds(setup.connectionTimeout));
        options.put(TypedDriverOption.REQUEST_TIMEOUT, java.time.Duration.ofSeconds(setup.requestTimeout));
        options.put(TypedDriverOption.REQUEST_CONSISTENCY, setup.consistencyLevel);
        //options.put(TypedDriverOption.PROTOCOL_COMPRESSION, "LZ4");
        //options.put(TypedDriverOption.PROTOCOL_COMPRESSION, "SNAPPY");
        options.put(TypedDriverOption.PROTOCOL_VERSION, "V4");
        builder.withConfigLoader(DriverConfigLoader.fromMap(options));

        return builder;
    }

    protected String prepareHeaders(String[] headers){
        return String.join(", ",headers);
    }

    protected String prepareItems(String[] headers){
        StringBuilder prepareItems= new StringBuilder();

        for (int i=0;i<headers.length;i++)
            prepareItems.append("?, ");
        return prepareItems.deleteCharAt(prepareItems.length() - 2).toString();
    }

    abstract void execute(String fileName) throws CsvValidationException, IOException;
}
