package org.george0st.processors.cql.processor;

import org.george0st.processors.cql.CqlAccess;
import org.george0st.processors.cql.helper.Setup;
import java.io.IOException;


/**
 * Basic abstract class for processor
 */
abstract class CqlProcessor extends CqlAccess {

    protected boolean dryRun;

    public CqlProcessor(Setup setup, boolean dryRun) {
        super(setup);
        this.dryRun=dryRun;
    }

    public CqlProcessor(CqlAccess access, boolean dryRun) {
        super(access);
        this.dryRun=dryRun;
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

    abstract long execute(String fileName) throws IOException;
    abstract long executeContent(String data) throws IOException;
    abstract long executeContent(byte[] byteArray) throws IOException;

}
