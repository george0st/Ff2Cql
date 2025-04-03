package org.george0st.processors.cql.processor;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.type.DataType;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.nifi.util.Tuple;
import org.george0st.processors.cql.helper.SetupRead;


import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * CSV read processor for reading data from table in CQL to the CSV.
 */
public class CsvCqlRead extends CqlProcessor {

    public class ReadResult {
        public ReadResult(String content, long rows) { this.content=content; this.rows=rows; }
        public String content;
        public long rows;
    }

    public CsvCqlRead(CqlSession session, SetupRead setup) { super(session, setup); }

    private long executeCore(Writer writer) throws IOException {
        List<String> columns=new ArrayList<>();
        long rowCount=0;
        ResultSet rs;

        //  execute CQL
        rs = session.execute(selectStatementSql(session, ((SetupRead)setup).columnNames, ((SetupRead)setup).whereClause));

        //  get columns
        for (ColumnDefinition cd: rs.getColumnDefinitions())
            columns.add(cd.getName().toString());

        //  create CSV format
        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                .setHeader(columns.toArray(new String[0]))
                .build();

        //  write CSV
        try (final CSVPrinter printer = new CSVPrinter(writer, csvFormat)) {
            String[] row=new String[columns.size()];
            for (Row rw : rs) {
                for (int i = 0; i < columns.size(); i++)
                    row[i]=rw.getString(i);
                printer.printRecord(row);
                rowCount++;
            }
        }
        return rowCount;
    }

    public ReadResult executeContent() throws IOException {
        try (Writer writer = new StringWriter()) {
            long rows = this.executeCore(writer);
            return new ReadResult(writer.toString(), rows);
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
