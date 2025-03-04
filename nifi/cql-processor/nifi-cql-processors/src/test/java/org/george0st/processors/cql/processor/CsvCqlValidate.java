package org.george0st.processors.cql.processor;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.DataType;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.george0st.processors.cql.helper.Setup;

import java.io.*;
import java.rmi.UnexpectedException;
import java.time.Instant;
import java.time.LocalTime;
import java.util.Iterator;


/**
 * CSV validation processor for checking, if content of CSV is the same as content in table in CQL
 */
public class CsvCqlValidate extends CqlProcessor {

    private final String[] primaryKeys;

    public CsvCqlValidate(CqlSession session, Setup setup, String [] primaryKeys) {
        super(session, setup);
        this.primaryKeys = primaryKeys;
    }

    public long executeCore(Reader reader) throws IOException {
        long totalCount=0;

        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setSkipHeaderRecord(true)
                .get();
        Iterator<CSVRecord> iterator = csvFormat.parse(reader).iterator();

        if (iterator.hasNext()) {
            String[] headers = iterator.next().values();
            String prepareHeaders = prepareHeaders(headers);
            String whereItems = whereItems(this.primaryKeys);
            int[] mapIndexes = mapIndexes(headers);

            PreparedStatement stm = selectStatement(session, prepareHeaders, whereItems);
            BoundStatement bound;

            String itm;
            String[] line;
            String[] newLine = new String[this.primaryKeys.length];
            Row row;
            DataType itmType;

            for ( ; iterator.hasNext(); ) {
                line = iterator.next().values();

                //  bind items for query
                for (int i : mapIndexes)
                    newLine[i] = line[i];
                bound = stm.bind((Object[]) newLine);
                totalCount++;

                // execute query
                row = session.execute(bound).one();
                if (row == null) continue;

                //  check values from query
                for (int i = 0; i < headers.length; i++) {
                    itmType = row.getType(i);
                    itm = row.getString(i);
                    if (itm != null) {
                        if (itmType == DataTypes.TIME) {
                            if (!LocalTime.parse(itm).equals(LocalTime.parse(line[i])))
                                throw new UnexpectedException(String.format("Check: Irrelevant values '%s','%s'", line[0], line[i]));
                        } else {
                            if (itmType == DataTypes.TIMESTAMP) {
                                if (!Instant.parse(itm).equals(Instant.parse(line[i]))) {
                                    throw new UnexpectedException(String.format("Check: Irrelevant values '%s','%s'", line[0], line[i]));
                                }
                            } else {
                                if (!itm.equals(line[i]))
                                    throw new UnexpectedException(String.format("Check: Irrelevant values '%s','%s'", line[0], line[i]));
                            }
                        }
                    }
                }
            }
        }
        return totalCount;
    }

    @Override
    protected long executeContent(String data) throws IOException {
        long totalCount=0;

        if (data!=null) {
            try (Reader reader = new StringReader(data)) {
                totalCount = this.executeCore(reader);
            }
        }
        return totalCount;
    }

    @Override
    protected long executeContent(byte[] byteArray) throws IOException {
        long totalCount=0;

        if (byteArray!=null) {
            try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArray)) {
                try (Reader reader = new InputStreamReader(byteArrayInputStream)) {
                    totalCount = this.executeCore(reader);
                }
            }
        }
        return totalCount;
    }

    public long execute(String fileName) throws IOException {
        long totalCount=0;

        if (fileName==null){
            // add stdin
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))){
                totalCount = this.executeCore(reader);
            }
        }
        else
            try (Reader reader = new FileReader(fileName)) {
                totalCount = this.executeCore(reader);
            }
        return totalCount;
    }

    protected int[] mapIndexes(String[] headers){
        int[] indexes =new int[this.primaryKeys.length];

        for (int j = 0; j < this.primaryKeys.length; j++) {
            for (int i = 0;i < headers.length;i++) {
                if (this.primaryKeys[j].equals(headers[i]))
                    indexes[j] = i;
            }
        }
        return indexes;
    }

    private PreparedStatement selectStatement(CqlSession session, String prepareHeaders, String whereItems){
        String selectQuery = "SELECT " + prepareHeaders + " FROM " + this.setup.table +
                " WHERE " + whereItems + ";";
        return session.prepare(SimpleStatement.newInstance(selectQuery)
                .setConsistencyLevel(DefaultConsistencyLevel.valueOf(this.setup.writeConsistencyLevel)));
    }
}
