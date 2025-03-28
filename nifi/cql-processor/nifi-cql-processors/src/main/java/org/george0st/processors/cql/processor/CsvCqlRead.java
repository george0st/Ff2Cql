package org.george0st.processors.cql.processor;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.type.DataType;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.nifi.util.Tuple;
import org.george0st.processors.cql.helper.SetupRead;


import java.io.*;
import java.util.Iterator;


/**
 * CSV read processor for reading data from table in CQL to the CSV.
 */
public class CsvCqlRead extends CqlProcessor {

    public CsvCqlRead(CqlSession session, SetupRead setup) { super(session, setup); }

    private long executeCore(Writer writer) throws IOException {
        long totalCount=0;
        BoundStatement bound;
        ResultSet rs;

//        PreparedStatement stm = selectStatement(session, ((SetupRead)setup).columnNames, ((SetupRead)setup).whereClause);
//        bound = stm.bind(null);
//        rs = session.execute(bound);
        rs = session.execute(selectStatementSql(session, ((SetupRead)setup).columnNames, ((SetupRead)setup).whereClause));
        long columnCount=0;
        StringBuilder stringBuilder=new StringBuilder();

        for (ColumnDefinition cd: rs.getColumnDefinitions()) {
            stringBuilder.append(stringBuilder.isEmpty() ?
                    String.format("\"%s\"",cd.getName()) :
                    String.format(",\"%s\"",cd.getName()));
            columnCount++;
        }
        writer.write(stringBuilder.toString()+"\r");
        System.out.println(stringBuilder.toString()+"\r");

        for (Row rw: rs){
            stringBuilder=new StringBuilder();
            for (int i=0;i<columnCount;i++) {
                stringBuilder.append(stringBuilder.isEmpty() ?
                        String.format("\"%s\"",rw.getString(i)) :
                        String.format(",\"%s\"",rw.getString(i)));
            }
            stringBuilder.append("\r");
            writer.write(stringBuilder.toString());
            System.out.println(stringBuilder.toString());
        }
//
//        String itm;
//        String[] line;
//        String[] newLine = new String[this.primaryKeys.length];
//        Row row;
//        DataType itmType;
//
//        for ( ; iterator.hasNext(); ) {
//            line = iterator.next().values();
//
//            //  bind items for query
//            for (int i : mapIndexes)
//                newLine[i] = line[i];
//            bound = stm.bind((Object[]) newLine);
//            totalCount++;
//
//            // execute query
//            row = session.execute(bound).one();







//        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
//                .setSkipHeaderRecord(true)
//                .get();
//        Iterator<CSVRecord> iterator = csvFormat.parse(reader).iterator();
//
//        if (iterator.hasNext()) {
//            String[] headers = iterator.next().values();
//            String prepareHeaders = prepareHeaders(headers);
//            String prepareItems = prepareItems(headers);
//            PreparedStatement stm = insertStatement(session, prepareHeaders, prepareItems);
//
//            BatchStatement batch = BatchStatement.newInstance(DefaultBatchType.valueOf(setup.batchType));
//            String[] line;
//            int count = 0;
//
//            while (iterator.hasNext()) {
//                line = iterator.next().values();
//                batch = batch.addAll(stm.bind((Object[]) line));
//                count++;
//                totalCount++;
//
//                if (count == setup.getBatchSize()) {
//                    if (!setup.dryRun)
//                        session.execute(batch);
//                    batch = batch.clear();
//                    count = 0;
//                }
//            }
//            if (count > 0)
//                if (!setup.dryRun)
//                    session.execute(batch);
//        }
        return totalCount;
    }

    public String executeContent() throws IOException {

        try (Writer writer = new StringWriter()) {
            this.executeCore(writer);
            return writer.toString();
        }
    }

//    public long executeContent(String data) throws IOException {
//        long totalCount=0;
//
//        if (data!=null) {
//            try (Reader reader = new StringReader(data)) {
//                totalCount = this.executeCore(reader);
//            }
//        }
//        return totalCount;
//    }

//    public long executeContent(byte[] byteArray) throws IOException {
//        long totalCount=0;
//
//        if (byteArray!=null) {
//            try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArray)) {
//                try (Reader reader = new InputStreamReader(byteArrayInputStream)) {
//                    totalCount = this.executeCore(reader);
//                }
//            }
//        }
//        return totalCount;
//    }

//    public long execute(String fileName) throws IOException {
//        long totalCount=0;
//
//        if (fileName==null){
//            // add stdin
//            try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))){
//                totalCount = this.executeCore(reader);
//            }
//        }
//        else
//            try (Reader reader = new FileReader(fileName)) {
//                totalCount = this.executeCore(reader);
//            }
//        return totalCount;
//    }

    private PreparedStatement selectStatement(CqlSession session, String prepareHeaders, String whereItems){
        return session.prepare(SimpleStatement.newInstance(selectStatementSql(session, prepareHeaders, whereItems))
                .setConsistencyLevel(DefaultConsistencyLevel.valueOf(this.setup.consistencyLevel)));
    }

    private String selectStatementSql(CqlSession session, String prepareHeaders, String whereItems){
        StringBuilder selectQuery = new StringBuilder();

        selectQuery.append("SELECT " + (prepareHeaders==null ? "*" : prepareHeaders));
        selectQuery.append(" FROM " + this.setup.table);
        if (whereItems!=null)
            selectQuery.append(" WHERE " + whereItems);
        selectQuery.append(";");
        return selectQuery.toString();
    }

}
