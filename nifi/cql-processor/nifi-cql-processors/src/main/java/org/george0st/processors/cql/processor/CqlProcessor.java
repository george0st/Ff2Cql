package org.george0st.processors.cql.processor;

import com.datastax.oss.driver.api.core.CqlSession;
import org.george0st.processors.cql.helper.Setup;
import org.george0st.processors.cql.helper.SetupWrite;

import java.io.IOException;


/**
 * Basic abstract class for processor
 */
public abstract class CqlProcessor {

    protected CqlSession session;
    protected Setup setup;

    public CqlProcessor(CqlSession cqlSession, Setup setup) {
        this.session = cqlSession;
        this.setup = setup;
    }

    protected String prepareHeaders(String[] headers){
        return String.join(", ",headers);
    }

    protected String prepareItems(String[] headers){
        StringBuilder prepareItems= new StringBuilder();

        for (int i=0;i<headers.length;i++)
            prepareItems.append("?, ");
        return prepareItems.deleteCharAt(prepareItems.length() - 2).toString();
    }

    protected String whereItems(String[] whereItems){
        StringBuilder prepareItems= new StringBuilder();

        for (int i=0;i<whereItems.length;i++){
            if (!prepareItems.isEmpty())
                prepareItems.append(" AND ");
            prepareItems.append(String.format("%s = ?", whereItems[i]));
        }
        return prepareItems.toString();
    }

//    protected abstract long execute(String fileName) throws IOException;
//    protected abstract long executeContent(String data) throws IOException;
//    protected abstract long executeContent(byte[] byteArray) throws IOException;

}
