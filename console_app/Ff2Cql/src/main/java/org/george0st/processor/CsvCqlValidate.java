package org.george0st.processor;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.type.DataTypes;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.george0st.helper.Setup;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.rmi.UnexpectedException;
import java.time.Instant;
import java.time.LocalTime;
import java.util.Iterator;


/**
 * CSV validation processor for checking, if content of CSV is the same as content in table in CQL
 */
public class CsvCqlValidate extends CqlProcessor {

    private final String[] readWhere;

    public CsvCqlValidate(Setup setup, String []readWhere) {
        super(setup, false);
        this.readWhere=readWhere;
    }

    public long execute(String fileName) throws IOException {
        long totalCount=0;

        try (CqlSession session = sessionBuilder.build()) {
            try (Reader reader = new FileReader(fileName)) {
                CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                        .setSkipHeaderRecord(true)
                        .get();
                Iterator<CSVRecord> iterator = csvFormat.parse(reader).iterator();

                if (iterator.hasNext()) {
                    String[] headers = iterator.next().values();
                    String prepareHeaders = prepareHeaders(headers);
                    String whereItems = whereItems(this.readWhere);
                    int[] mapIndexes = mapIndexes(headers);

                    PreparedStatement stm = selectStatement(session, prepareHeaders, whereItems);
                    BoundStatement bound;

                    String itm;
                    String[] line;
                    String[] newLine = new String[this.readWhere.length];
                    Row row;
                    com.datastax.oss.driver.api.core.type.DataType itmType;
                    for (; iterator.hasNext(); ) {
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
            }
        }
        return totalCount;
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
