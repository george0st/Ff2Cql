package org.george0st.processors.cql.helper;

import org.apache.nifi.processor.ProcessContext;
import org.george0st.processors.cql.PutCQL;


/**
 * The definition of ControllerSetup (load setting from *.json file)
 */
public class SetupWrite extends Setup {

    private long batchSize;
    public String batchType;
    public boolean dryRun;

    public void setBatchSize(long batchSize) { this.batchSize = batchSize; }
    public long getBatchSize() { return batchSize > 0 ? batchSize : 200; }

    public SetupWrite(){ super(); }

    /**
     * Constructor with process context setting
     *
     * @param context   definition of process context
     */
    public SetupWrite(ProcessContext context){
        consistencyLevel = context.getProperty(PutCQL.CONSISTENCY_LEVEL).getValue();
        table = context.getProperty(PutCQL.TABLE).evaluateAttributeExpressions().getValue();

        setBatchSize(context.getProperty(PutCQL.BATCH_SIZE).evaluateAttributeExpressions().asLong());
        batchType=context.getProperty(PutCQL.BATCH_TYPE).getValue();
        dryRun = context.getProperty(PutCQL.DRY_RUN).asBoolean();
    }
}
