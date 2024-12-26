package org.george0st.processor;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import org.george0st.helper.Setup;

import javax.management.InvalidAttributeValueException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;


public class CsvCqlValidate extends CqlProcessor {

    private String[] readWhere;

    public CsvCqlValidate(Setup setup, String []readWhere) {
        super(setup);
        this.readWhere=readWhere;
    }

    public void execute(String fileName) throws CsvValidationException, IOException, InvalidAttributeValueException {
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
                    BoundStatement bound=null;

                    String itm;
                    String[] line= null;
                    String[] newLine= new String[this.readWhere.length];
                    Row row;
                    com.datastax.oss.driver.api.core.type.DataType itmType;

                    while ((line = csvReader.readNext()) != null) {
                        //  bind items for query
                        for (int i: mapIndexes)
                            newLine[i]=line[i];
                        bound=stm.bind(newLine);

                        // execute query
                        row=session.execute(bound).one();

                        //  check values from query
                        for (int i=0;i<headers.length; i++) {
                            itmType = row.getType(i);
                            itm = row.getString(i);
                            if (itm!=null) {
                                if (itmType == DataTypes.TIME) {
                                    if (!LocalTime.parse(itm).equals(LocalTime.parse(line[i])))
                                        throw new InvalidAttributeValueException("Check: Irrelevant values");
                                } else {
                                    if (itmType == DataTypes.TIMESTAMP) {
                                        if (!LocalDateTime.parse(itm).equals(LocalDateTime.parse(line[i]))) {
                                            LocalDateTime iii= LocalDateTime.parse(itm);
                                            LocalDateTime iii2=iii.atZone(ZoneId.of("Europe/London")).toLocalDateTime();

                                            throw new InvalidAttributeValueException("Check: Irrelevant values");

                                        }
                                    } else {
                                        if (!itm.equals(line[i]))
                                            throw new InvalidAttributeValueException("Check: Irrelevant values");
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
        String selectQuery = new StringBuilder("")
                .append(String.format("SELECT %s ", prepareHeaders))
                .append("FROM ")
                .append(this.setup.table)
                .append(String.format(" WHERE %s", whereItems)).toString();
        return session.prepare(SimpleStatement.newInstance(selectQuery)
                .setConsistencyLevel(DefaultConsistencyLevel.valueOf(this.setup.consistencyLevel)));
    }
}
