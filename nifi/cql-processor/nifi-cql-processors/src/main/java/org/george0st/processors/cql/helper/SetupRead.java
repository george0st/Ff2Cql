package org.george0st.processors.cql.helper;

import org.apache.nifi.processor.ProcessContext;
import org.george0st.processors.cql.GetCQL;


/**
 * The definition of ControllerSetup (load setting from *.json file)
 */
public class SetupRead extends Setup {

    public SetupRead(){ super();  }

    /**
     * Constructor with process context setting
     *
     * @param context   definition of process context
     */
    public SetupRead(ProcessContext context){
        consistencyLevel = context.getProperty(GetCQL.CONSISTENCY_LEVEL).getValue();
        table = context.getProperty(GetCQL.TABLE).evaluateAttributeExpressions().getValue();
    }
}
