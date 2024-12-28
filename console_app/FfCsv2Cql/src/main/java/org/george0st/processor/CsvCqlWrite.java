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


public class CsvCqlWrite extends CqlProcessor {

    public CsvCqlWrite(Setup setup) {
        super(setup);
    }

    public CsvCqlWrite(CqlAccess access) {
        super(access);
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
                    String[] line;
                    int count=0;

                    while ((line = csvReader.readNext()) != null) {
                        batch = batch.addAll(stm.bind((Object[]) line));
                        count++;

                        if (count==setup.getBulk()) {
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
        String insertQuery = "INSERT INTO " + this.setup.table + " (" + prepareHeaders+") " +
                "VALUES (" + prepareItems + ");";
        return session.prepare(SimpleStatement.newInstance(insertQuery)
                .setConsistencyLevel(DefaultConsistencyLevel.valueOf(this.setup.consistencyLevel)));
    }

}
