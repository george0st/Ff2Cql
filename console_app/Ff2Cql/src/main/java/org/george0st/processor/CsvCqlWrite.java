package org.george0st.processor;

import java.io.*;
import java.util.Iterator;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.*;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.george0st.CqlAccess;
import org.george0st.helper.Setup;


/**
 * CSV write processor for writing data from CSV to the table in CQL.
 */
public class CsvCqlWrite extends CqlProcessor {

    public CsvCqlWrite(Setup setup, boolean dryRun) {
        super(setup, dryRun);
    }

    public CsvCqlWrite(CqlAccess access, boolean dryRun) {
        super(access, dryRun);
    }

    private long executeCore(CqlSession session, Reader reader) throws IOException, CsvValidationException {
        long totalCount=0;

        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setSkipHeaderRecord(true)
                .build();
        Iterator<CSVRecord> iterator = csvFormat.parse(reader).iterator();

        String[] headers = iterator.next().values();
        String prepareHeaders = prepareHeaders(headers);
        String prepareItems = prepareItems(headers);
        PreparedStatement stm = insertStatement(session, prepareHeaders, prepareItems);

        BatchStatement batch = BatchStatement.newInstance(DefaultBatchType.UNLOGGED);
        String[] line;
        int count=0;

        for (;iterator.hasNext();) {
            line = iterator.next().values();
            batch = batch.addAll(stm.bind((Object[]) line));
            count++;
            totalCount++;

            if (count==setup.getBulk()) {
                if (!dryRun)
                    session.execute(batch);
                batch = batch.clear();
                count = 0;
            }
        }
        if (count > 0)
            if (!dryRun)
                session.execute(batch);
        return totalCount;
    }

    public long execute(String fileName) throws CsvValidationException, IOException {
        long totalCount=0;

        try (CqlSession session = sessionBuilder.build()) {

            if (fileName==null){
                // add stdin
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))){
                    totalCount = this.executeCore(session, reader);
                }
            }
            else
                try (Reader reader = new FileReader(fileName)) {
                    totalCount = this.executeCore(session, reader);
            }
        }
        return totalCount;
    }

    private PreparedStatement insertStatement(CqlSession session, String prepareHeaders, String prepareItems){
        String insertQuery = "INSERT INTO " + this.setup.table + " (" + prepareHeaders+") " +
                "VALUES (" + prepareItems + ");";
        return session.prepare(SimpleStatement.newInstance(insertQuery)
                .setConsistencyLevel(DefaultConsistencyLevel.valueOf(this.setup.consistencyLevel)));
    }

}
