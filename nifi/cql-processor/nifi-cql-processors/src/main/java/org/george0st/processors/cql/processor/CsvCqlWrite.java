package org.george0st.processors.cql.processor;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.george0st.processors.cql.CqlAccess;
import org.george0st.processors.cql.helper.Setup;
import java.io.*;
import java.util.Iterator;


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

    private long executeCore(CqlSession session, Reader reader) throws IOException {
        long totalCount=0;

        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setSkipHeaderRecord(true)
                .get();
        Iterator<CSVRecord> iterator = csvFormat.parse(reader).iterator();

        if (iterator.hasNext()) {
            String[] headers = iterator.next().values();
            String prepareHeaders = prepareHeaders(headers);
            String prepareItems = prepareItems(headers);
            PreparedStatement stm = insertStatement(session, prepareHeaders, prepareItems);

            BatchStatement batch = BatchStatement.newInstance(DefaultBatchType.UNLOGGED);
            String[] line;
            int count = 0;

            while (iterator.hasNext()) {
                line = iterator.next().values();
                batch = batch.addAll(stm.bind((Object[]) line));
                count++;
                totalCount++;

                if (count == setup.getBatch()) {
                    if (!dryRun)
                        session.execute(batch);
                    batch = batch.clear();
                    count = 0;
                }
            }
            if (count > 0)
                if (!dryRun)
                    session.execute(batch);
        }
        return totalCount;
    }

    public long executeContent(String data) throws IOException {
        long totalCount=0;

        try (CqlSession session = sessionBuilder.build()) {
            if (data!=null) {
                try (Reader reader = new StringReader(data)) {
                    totalCount = this.executeCore(session, reader);
                }
            }
        }
        return totalCount;
    }

    public long executeContent(byte[] byteArray) throws IOException {
        long totalCount=0;

        try (CqlSession session = sessionBuilder.build()) {
            if (byteArray!=null) {
                try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArray)) {
                    try (Reader reader = new InputStreamReader(byteArrayInputStream)) {
                        totalCount = this.executeCore(session, reader);
                    }
                }
            }
        }
        return totalCount;
    }

    public long execute(String fileName) throws IOException {
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
