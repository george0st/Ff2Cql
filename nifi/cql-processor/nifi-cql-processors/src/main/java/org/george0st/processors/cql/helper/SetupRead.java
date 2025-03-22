package org.george0st.processors.cql.helper;

import org.apache.nifi.processor.ProcessContext;
import org.george0st.processors.cql.GetCQL;
import org.george0st.processors.cql.PutCQL;


/**
 * The definition of ControllerSetup (load setting from *.json file)
 */
public class SetupRead {

    public String readConsistencyLevel;
    public String table;

    public String getOnlyTable() { return table!=null ? table.split("\\.")[1] : null; }
    public String getOnlyKeyspace() { return table!=null ? table.split("\\.")[0] : null; }

    public SetupRead(){
    }

    /**
     * Constructor with process context setting
     *
     * @param context   definition of process context
     */
    public SetupRead(ProcessContext context){
        readConsistencyLevel = context.getProperty(GetCQL.READ_CONSISTENCY_LEVEL).getValue();
        table = context.getProperty(GetCQL.TABLE).evaluateAttributeExpressions().getValue();
    }
}
