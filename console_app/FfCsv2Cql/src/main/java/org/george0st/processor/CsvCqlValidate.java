package org.george0st.processor;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import org.george0st.helper.Setup;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.rmi.UnexpectedException;
import java.time.Instant;
import java.time.LocalTime;


public class CsvCqlValidate extends CqlProcessor {

    private final String[] readWhere;

    public CsvCqlValidate(Setup setup, String []readWhere) {
        super(setup, false);
        this.readWhere=readWhere;
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
                    String whereItems = whereItems(this.readWhere);
                    int[] mapIndexes = mapIndexes(headers);

                    PreparedStatement stm = selectStatement(session, prepareHeaders, whereItems);
                    BoundStatement bound;

                    String itm;
                    String[] line;
                    String[] newLine= new String[this.readWhere.length];
                    Row row;
                    com.datastax.oss.driver.api.core.type.DataType itmType;

                    while ((line = csvReader.readNext()) != null) {
                        //  bind items for query
                        for (int i: mapIndexes)
                            newLine[i]=line[i];
                        bound=stm.bind((Object[]) newLine);

                        // execute query
                        row = session.execute(bound).one();
                        if (row==null) continue;

                        //  check values from query
                        for (int i=0;i<headers.length; i++) {
                            itmType = row.getType(i);
                            itm = row.getString(i);
                            if (itm!=null) {
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
            }
        }
    }

    protected int[] mapIndexes(String[] headers){
        int[] indexes =new int[this.readWhere.length];

        for (int j=0;j<this.readWhere.length;j++) {
            for (int i=0;i<headers.length;i++) {
                if (this.readWhere[j].equals(headers[i]))
                    indexes[j]=i;
            }
        }
        return indexes;
    }

    private PreparedStatement selectStatement(CqlSession session, String prepareHeaders, String whereItems){

        String selectQuery = "SELECT " + prepareHeaders + " FROM " + this.setup.table +
                " WHERE " + whereItems + ";";
        return session.prepare(SimpleStatement.newInstance(selectQuery)
                .setConsistencyLevel(DefaultConsistencyLevel.valueOf(this.setup.consistencyLevel)));
    }
}
