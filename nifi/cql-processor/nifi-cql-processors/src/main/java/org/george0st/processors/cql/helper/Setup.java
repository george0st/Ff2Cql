package org.george0st.processors.cql.helper;

import org.apache.nifi.processor.ProcessContext;
import org.george0st.processors.cql.PutCQL;


/**
 * The definition of ControllerSetup (load setting from *.json file)
 */
public class Setup {

    public String consistencyLevel;
    public String table;

    public String getOnlyTable() { return table!=null ? table.split("\\.")[1] : null; }
    public String getOnlyKeyspace() { return table!=null ? table.split("\\.")[0] : null; }

    public Setup(){
    }
}
