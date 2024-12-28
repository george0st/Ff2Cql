package org.george0st.processor;

import java.io.IOException;
import java.io.Reader;
import java.util.Collections;


import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.cql.*;
import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReaderBuilder;
import java.io.FileReader;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
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
                    String[] line= null;
                    int count=0;

                    while ((line = csvReader.readNext()) != null) {
                        batch = batch.addAll(stm.bind(line));
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
        String insertQuery = new StringBuilder("")
                .append("INSERT INTO ")
                .append(this.setup.table)
                .append(String.format(" (%s) ",prepareHeaders))
                .append("VALUES ")
                .append(String.format("(%s);",prepareItems)).toString();

        return session.prepare(SimpleStatement.newInstance(insertQuery)
                .setConsistencyLevel(DefaultConsistencyLevel.valueOf(this.setup.consistencyLevel)));
    }

}
