package org.george0st;

import com.datastax.oss.driver.api.core.CqlSession;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import org.george0st.helper.RndGenerator;
import org.george0st.helper.Setup;

import javax.management.InvalidAttributeValueException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class CqlCreateSchema extends CqlAccess {

    private RndGenerator rnd=new RndGenerator();
    private static String testOutput="./test_output";
    private static String testInput="./test_input";

    private String[] primaryKeys=new String[]{"colbigint", "colint"};
    private String[] columns=new String[]{
            "colbigint","BIGINT",
            "colint", "INT",
            "coltext", "TEXT",
            "colfloat", "FLOAT",
            "coldouble", "DOUBLE",
            "coldate", "DATE",
            "coltime", "TIME"};

    public CqlCreateSchema(Setup setup) throws InterruptedException {
        super(setup);
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



    public void Create() throws CsvValidationException, IOException, InvalidAttributeValueException {
        try (CqlSession session = sessionBuilder.build()) {
//            // Drop key space
//            session.execute(f"DROP KEYSPACE IF EXISTS {self._run_setup['keyspace']};");
//
//            // Create key space
//            session.execute(f"CREATE KEYSPACE IF NOT EXISTS {self._run_setup['keyspace']} " +
//                    "WITH replication = {" +
//                    f"'class':'{self._run_setup['keyspace_replication_class']}', " +
//                    f"'replication_factor' : {self._run_setup['keyspace_replication_factor']}" +
//                    "};");

            // DROP TABLE
            session.execute(String.format("DROP TABLE IF EXISTS %s;", setup.table));

            // CREATE TABLE
            String createTable = new StringBuilder()
                    .append(String.format("CREATE TABLE IF NOT EXISTS %s ", setup.table))
                    //.append("(colbigint BIGINT, colint INT, coltext TEXT, ")
                    .append(String.format("(%s ", getColumnDefinitions()))
                    //.append("PRIMARY KEY(colbigint, colint)) ")
                    .append(String.format("PRIMARY KEY (%s)) ", String.join(",",primaryKeys)))
                    .append("WITH compaction = {'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'L10, T10'}")
                    .toString();
            session.execute(createTable);
        }
    }



    public File generateRndCSVFile(int csvItems, boolean sequenceID) throws IOException {
        // generate random file name
        File randomFile=getRandomFile();
        int randomIDRange = csvItems * csvItems;

        // generate random content
        try (FileWriter writer =new FileWriter(randomFile,false)){
            try (CSVWriter csvWriter = new CSVWriter(writer)){

                // write header
                String[] aa=getColumns();
                csvWriter.writeNext(aa);

                // write content
                for (int i=0;i<csvItems;i++) {
                    csvWriter.writeNext(new String[]{
                            sequenceID ? Integer.toString(i) : Integer.toString(rnd.getInt(randomIDRange)),
                            Integer.toString(rnd.getInt(randomIDRange)),
                            rnd.getStringSequence(10),
                            Float.toString(rnd.getFloat(1000)),
                            Double.toString(rnd.getDouble(1000)),
                            "2024-12-24",
                            "17:45:30"});
                }
            }
        }
        return randomFile;
    }
}
