package org.george0st.processors.cql.helper;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.george0st.processors.cql.processor.CqlProcessor;

import java.io.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;


/**
 * Create test table in CQL for complex testing of all supported types.
 */
public class CqlTestSchema extends CqlProcessor {

    private final RndGenerator rnd=new RndGenerator();
    private final static String testOutput="./test_output";
    private final static String testInput="./test_input";
    private final long operationSleepTime=2000;

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

    public CqlTestSchema() throws InterruptedException {
        super(null, null);
    }

    public CqlTestSchema(CqlSession session, TestSetup setup) throws InterruptedException {
        super(session, setup);
    }

    public String[] getPrimaryKeys(){
        return primaryKeys;
    }

    private String getColumnDefinitions(){
        StringBuilder bld=new StringBuilder();
        for (int i=0;i<columns.length/2;i++)
            bld.append(String.format("%s %s,", columns[i*2],columns[i*2+1]));
        return bld.toString();
    }

    private String[] getColumns(){
        List<String> bld=new ArrayList<String>();
        for (int i=0;i<columns.length/2;i++)
            bld.add(columns[i*2]);
        return bld.toArray(new String[0]);
    }

    protected File getRandomFile(){
        return new File(String.format("%s/CsvToCql_%s.csv.tmp",testOutput, rnd.getStringSequence(10)));
    }

    /**
     * Create test schema in CQL
     */
    public boolean createSchema() {
        boolean newSchema=false;

        if (!requestedKeyspace(setup.getOnlyKeyspace())) {
            // Create key space, if not exist
            session.execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = %s;",
                    setup.getOnlyKeyspace(),
                    ((TestSetup) setup).replication));
            newSchema=true;
        }

        if (!requestedTable(setup.getOnlyKeyspace(), setup.getOnlyTable())) {
            // DROP TABLE
            session.execute(String.format("DROP TABLE IF EXISTS %s;", setup.table));

            // CREATE TABLE
            String createTable = String.format("CREATE TABLE IF NOT EXISTS %s ", setup.table) +
                    String.format("(%s ", getColumnDefinitions()) +
                    String.format("PRIMARY KEY (%s)) ", String.join(",", primaryKeys)) +
                    String.format("WITH compaction = %s;", ((TestSetup) setup).compaction);
            session.execute(createTable);
            newSchema=true;
        }
        return newSchema;
    }

    /**
     * Clean test data in CQL (keep only empty schema)
     */
    public void cleanData() throws InterruptedException {
        try {
            session.execute(String.format("TRUNCATE TABLE %s;", setup.table));
            //  for data synchronization
            Thread.sleep(operationSleepTime);
        }
        catch(Exception ignored){}
    }

    /**
     * Check, if keyspace exist
     *
     * @param keyspaceName  tested key space
     * @return  true - the keyspace exist, false - the keyspace does not exist
     */
    private boolean requestedKeyspace(String keyspaceName){
        try {
            return session.execute(String.format("SELECT keyspace_name FROM system_schema.keyspaces "+
                    "WHERE keyspace_name='%s';", keyspaceName)).one() != null;
        }
        catch(Exception ignored){}
        return false;
    }

    /**
     * Check, if table has the requested structure
     *
     * @param keyspaceName  tested key space
     * @param tableName     tested table
     * @return  true - the requested content, false - different content
     */
    private boolean requestedTable(String keyspaceName, String tableName){
        boolean result=false;

        try {
            ResultSet rs = session.execute(String.format("SELECT column_name, kind, type " +
                    "FROM system_schema.columns " +
                    "WHERE keyspace_name = '%s' AND table_name = '%s'", keyspaceName, tableName));

            List<String> dbColumns=new ArrayList<>();
            for (Row row: rs) dbColumns.add(row.getString("column_name"));

            result=true;
            for (String column: getColumns()) {
                if (!dbColumns.contains(column)) {
                    result = false;
                    break;
                }
            }
        }
        catch(Exception ignored){}
        return result;
    }

    private void generateRndContent(Writer writer, int csvItems, boolean sequenceID) throws IOException {
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

    /**
     * Generate random test data as CSV String
     *
     * @param csvItems      amount of items
     * @param sequenceID    sequence ID
     * @return              generated data
     * @throws IOException  exception from commons-csv
     */
    public String generateRndCSVString(int csvItems, boolean sequenceID) throws IOException {
        // generate random content
        try (StringWriter writer = new StringWriter()) {
            generateRndContent(writer, csvItems, sequenceID);
            return writer.toString();
        }
    }

    /**
     * Generated random test data as CSV file
     *
     * @param csvItems      amount of items
     * @param sequenceID    sequence ID
     * @return              the File with random data
     * @throws IOException  exception from commons-csv
     */
    public File generateRndCSVFile(int csvItems, boolean sequenceID) throws IOException {
        // generate random file
        File randomFile=getRandomFile();

        // generate random content
        try (FileWriter writer = new FileWriter(randomFile, false)) {
            generateRndContent(writer, csvItems, sequenceID);
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
