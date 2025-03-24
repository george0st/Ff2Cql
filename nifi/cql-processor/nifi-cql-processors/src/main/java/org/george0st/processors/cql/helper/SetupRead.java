package org.george0st.processors.cql.helper;

import org.apache.nifi.processor.ProcessContext;
import org.george0st.processors.cql.GetCQL;


/**
 * The definition of ControllerSetup (load setting from *.json file)
 */
public class SetupRead extends Setup {

    public String columnNames;
    public String whereClause;
    public String cqlQuery;

    public SetupRead(){ super();  }

    /**
     * Constructor with process context setting
     *
     * @param context   definition of process context
     */
    public SetupRead(ProcessContext context){
        consistencyLevel = context.getProperty(GetCQL.CONSISTENCY_LEVEL).getValue();
        table = context.getProperty(GetCQL.TABLE).evaluateAttributeExpressions().getValue();

        columnNames = context.getProperty(GetCQL.COLUMN_NAMES).evaluateAttributeExpressions().getValue();
        whereClause = context.getProperty(GetCQL.WHERE_CLAUSE).evaluateAttributeExpressions().getValue();
        cqlQuery = context.getProperty(GetCQL.CQL_QUERY).evaluateAttributeExpressions().getValue();
    }
}
