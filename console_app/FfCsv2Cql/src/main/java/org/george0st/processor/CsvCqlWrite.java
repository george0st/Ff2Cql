package org.george0st.processor;

import java.io.IOException;
import java.io.Reader;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.*;
import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReaderBuilder;
import java.io.FileReader;
import com.opencsv.exceptions.CsvValidationException;
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

    public long execute(String fileName) throws CsvValidationException, IOException {
        long totalCount=0;

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
                    String[] line;
                    int count=0;

                    while ((line = csvReader.readNext()) != null) {
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
                }
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
