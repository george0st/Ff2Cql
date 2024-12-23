package org.george0st.processor;

import java.io.IOException;
import java.io.Reader;
import java.net.InetSocketAddress;
import java.util.Collections;


import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
//import com.datastax.oss.driver.api.core.config.TypedDriverOption;
//import com.datastax.oss.driver.api.core.config.
import com.datastax.oss.driver.api.core.cql.*;
import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReaderBuilder;
import java.io.FileReader;

import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.opencsv.exceptions.CsvValidationException;
import org.george0st.helper.Setup;
import org.george0st.codec.CqlBigIntToStringCodec;
import org.george0st.codec.CqlIntToStringCodec;

//import com.datastax.oss.driver.api.core.*;
//import com.datastax.oss.driver.api.core .Cluster.Builder

//import com.datastax.oss.driver.api.core.policies.DCAwareRoundRobinPolicy;
//import com.datastax.oss.driver.api.core.policies.HostFilterPolicy;
//import com.datastax.oss.driver.api.core.policies.RoundRobinPolicy;



public class CsvCqlWrite extends CqlProcessor {

    public CsvCqlWrite(Setup setup) {
        super(setup);
    }

    public void execute(String fileName) throws CsvValidationException, IOException {
        try (CqlSession session = sessionBuilder.build()) {
            try (Reader reader = new FileReader(fileName)) {
                CSVParser parser = new CSVParserBuilder()
                        .withSeparator(',')
                        .build();

                try (CSVReader csvReader = new CSVReaderBuilder(reader)
                        .withSkipLines(0)
                        .withCSVParser(parser)
                        .build()){

                    String[] headers = csvReader.readNext();
                    String prepareHeaders = prepareHeaders(headers);
                    String prepareItems = prepareItems(headers);
                    PreparedStatement stm = insertStatement(session,prepareHeaders, prepareItems);

                    BatchStatement batch = BatchStatement.newInstance(DefaultBatchType.UNLOGGED);
                    String[] line= null;
                    int count=0;

                    while ((line = csvReader.readNext()) != null) {
                        batch = batch.addAll(stm.bind(line));
                        count++;

                        if (count==setup.bulk) {
                            session.execute(batch);
                            batch = batch.clear();
                            count = 0;
                        }
                    }
                    if (count > 0)
                        session.execute(batch);
                }
            }
        }
    }

    private PreparedStatement insertStatement(CqlSession session, String prepareHeaders, String prepareItems){

        String insertQuery = new StringBuilder("")
                .append("INSERT INTO ")
                .append(this.setup.table)
                .append(String.format(" (%s) ",prepareHeaders))
                .append("VALUES ")
                .append(String.format("(%s);",prepareItems)).toString();
        return session.prepare(insertQuery);
    }

    public void connect(){
        try (CqlSession session = sessionBuilder.build()) {
            // We use execute to send a query to Cassandra. This returns a ResultSet, which
            // is essentially a collection of Row objects.

            // Consistency level for batch
//            BatchStatement batch =
//                    BatchStatement.newInstance(UNLOGGED).setConsistencyLevel();

            // Consistency level for read
//            SimpleStatement stmt =
//                    SimpleStatement.newInstance(
//                                    "SELECT sensor_id, date, timestamp, value "
//                                            + "FROM downgrading.sensor_data "
//                                            + "WHERE "
//                                            + "sensor_id = 756716f7-2e54-4715-9f00-91dcbea6cf50 AND "
//                                            + "date = '2018-02-26' AND "
//                                            + "timestamp > '2018-02-26+01:00'")
//                            .setConsistencyLevel(cl);


            ResultSet rs = session.execute("select release_version from system.local");
            //  Extract the first row (which is the only one in this case).
            Row row = rs.one();

            // Extract the value of the first (and only) column from the row.
            assert row != null;
            String releaseVersion = row.getString("release_version");
            System.out.printf("Cassandra version is: %s%n", releaseVersion);
        }
    }

    public void test() {

 /*
    datastax-java-driver {
      basic.contact-points = [ "127.0.0.1:9042" ]
      basic.load-balancing-policy.local-datacenter = "dc1"
      basic.request.consistency = LOCAL_QUORUM
      profiles {
        remote {
          basic.load-balancing-policy.local-datacenter = "dc2"
          basic.request.consistency = LOCAL_ONE
        }
      }
    }
    */

        OptionsMap options = OptionsMap.driverDefaults();

        // set the datacenter to dc1 in the default profile; this makes dc1 the local datacenter
        options.put(TypedDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "dc1");
        options.put(TypedDriverOption.CONNECTION_CONNECT_TIMEOUT, java.time.Duration.ofSeconds(5));
        // set the datacenter to dc2 in the "remote" profile
        options.put("remote", TypedDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "dc2");
        // make sure to provide a contact point belonging to dc1, not dc2!
        options.put(TypedDriverOption.CONTACT_POINTS, Collections.singletonList("127.0.0.1:9042"));
        // in this example, the default consistency level is LOCAL_QUORUM
        options.put(TypedDriverOption.REQUEST_CONSISTENCY, "LOCAL_QUORUM");
        // but when failing over, the consistency level will be automatically downgraded to LOCAL_ONE
        options.put("remote", TypedDriverOption.REQUEST_CONSISTENCY, "LOCAL_ONE");

        CqlSession session = CqlSession.builder().withConfigLoader(DriverConfigLoader.fromMap(options)).build();
    }

}
