package org.george0st.processors.cql.helper;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.george0st.processors.cql.helper.RndGenerator;
import org.george0st.processors.cql.helper.TestSetup;
import org.george0st.processors.cql.processor.CqlProcessor;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * Create test table in CQL for complex testing of all supported types.
 */
public class CqlCreateSchema extends CqlProcessor {

    private final RndGenerator rnd=new RndGenerator();
    private final static String testOutput="./test_output";
    private final static String testInput="./test_input";

    private final String[] primaryKeys=new String[]{"colbigint", "colint"};
    private final String[] columns=new String[]{
            "colbigint", "bigint",
            "colint", "int",
            "coltext", "text",
            "colfloat", "float",
            "coldouble", "double",
            "coldate", "date",
            "coltime", "time",
            "coltimestamp", "timestamp",
            "colboolean", "boolean",
            "coluuid", "uuid",
            "colsmallint", "smallint",
            "coltinyint", "tinyint",
            "coltimeuuid", "timeuuid",
            "colvarchar", "varchar"};

    public CqlCreateSchema(CqlSession session, TestSetup setup) throws InterruptedException {
        super(session, setup);
    }

    public String[] getPrimaryKeys(){
        return primaryKeys;
    }

    public String getColumnDefinitions(){
        StringBuilder bld=new StringBuilder();
        for (int i=0;i<columns.length/2;i++)
            bld.append(String.format("%s %s,", columns[i*2],columns[i*2+1]));
        return bld.toString();
    }

    public String[] getColumns(){
        List<String> bld=new ArrayList<String>();
        for (int i=0;i<columns.length/2;i++)
            bld.add(columns[i*2]);
        return bld.toArray(new String[0]);
    }

    protected File getRandomFile(){
        return new File(String.format("%s/CsvToCql_%s.csv.tmp",testOutput, rnd.getStringSequence(10)));
    }

    public void Create() {
//            // Drop key space
//            session.execute(f"DROP KEYSPACE IF EXISTS {self._run_setup['keyspace']};");

//            // Create key space
//            session.execute(f"CREATE KEYSPACE IF NOT EXISTS {self._run_setup['keyspace']} " +
//                    "WITH replication = {" +
//                    f"'class':'{self._run_setup['keyspace_replication_class']}', " +
//                    f"'replication_factor' : {self._run_setup['keyspace_replication_factor']}" +
//                    "};");

        // DROP TABLE
        session.execute(String.format("DROP TABLE IF EXISTS %s;", setup.table));

        // CREATE TABLE
        String createTable = String.format("CREATE TABLE IF NOT EXISTS %s ", setup.table) +
                String.format("(%s ", getColumnDefinitions()) +
                String.format("PRIMARY KEY (%s)) ", String.join(",", primaryKeys)) +
                String.format("WITH compaction = %s;", ((TestSetup) setup).compaction);
        session.execute(createTable);
    }

    public File generateRndCSVFile(int csvItems, boolean sequenceID) throws IOException {
        // generate random file name
        File randomFile=getRandomFile();

            // generate random content
        try (FileWriter writer = new FileWriter(randomFile, false)) {
            if (csvItems>=0) {
                CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                        .setHeader(getColumns())
                        .get();

                try (final CSVPrinter csvWriter = new CSVPrinter(writer, csvFormat)) {
                    // write content
                    for (int i = 0; i < csvItems; i++) {
                        csvWriter.printRecord(
                                sequenceID ? Integer.toString(i) : Integer.toString(rnd.getInt(Integer.MAX_VALUE)), //  bigint
                                Integer.toString(rnd.getInt(Integer.MAX_VALUE)),                    //  int
                                rnd.getStringSequence(10),                              // text
                                Float.toString(rnd.getFloat(1000)),                     // float
                                Double.toString(rnd.getDouble(1000)),                   //  double
                                rnd.getLocalDate().format(DateTimeFormatter.ISO_LOCAL_DATE),    //  date
                                rnd.getLocalTime().format(DateTimeFormatter.ISO_LOCAL_TIME),    //  time
                                rnd.getInstant().toString(),                                     //  timestamp
                                rnd.getBoolean().toString(),                                     //  boolean
                                rnd.getUUID(false).toString(),                          //  uuid
                                Integer.toString(rnd.getInt(0, Short.MAX_VALUE)),                //  smallint
                                Integer.toString(rnd.getInt(0, Byte.MAX_VALUE)),                 //  tinyint
                                rnd.getUUID(true).toString(),                           //  timeuuid
                                rnd.getStringSequence(5));                                //  varchar
                    }
                }
            }
        }
        return randomFile;
    }

    public long execute(String fileName) throws IOException{
        return 0;
    }

    public long executeContent(String data) throws IOException{
        return 0;
    }

    public long executeContent(byte[] byteArray) throws IOException{
        return 0;
    }

}
