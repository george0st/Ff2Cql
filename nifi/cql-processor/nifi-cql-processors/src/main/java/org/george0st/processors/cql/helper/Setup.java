package org.george0st.processors.cql.helper;

import org.apache.nifi.processor.ProcessContext;
import org.george0st.processors.cql.PutCQL;


/**
 * The definition of ControllerSetup (load setting from *.json file)
 */
public class Setup {
    public enum CompareStatus {
        SAME,
        CHANGE,
        CHANGE_ACCESS;
    }

    public String writeConsistencyLevel;
    private long batchSize;
    public String table;
    public boolean dryRun;

    public void setBatchSize(long batchSize) { this.batchSize = batchSize; }
    public long getBatchSize() { return batchSize > 0 ? batchSize : 200; }

    public Setup(){
    }

    /**
     * Constructor with process context setting
     *
     * @param context   definition of process context
     */
    public Setup(ProcessContext context){
        writeConsistencyLevel = context.getProperty(PutCQL.WRITE_CONSISTENCY_LEVEL).getValue();
        table = context.getProperty(PutCQL.TABLE).getValue();
        setBatchSize(context.getProperty(PutCQL.BATCH_SIZE).asLong());
        dryRun = context.getProperty(PutCQL.DRY_RUN).asBoolean();
    }

//    public CompareStatus compare(ControllerSetup o){
//
//        //  check null
//        if (o == null) return CompareStatus.CHANGE_ACCESS;
//
//        // If the object is compared with itself then return true
//        if (o == this) return CompareStatus.SAME;
//
//        //  change in Access level
//        if (!Arrays.equals(o.ipAddresses, ipAddresses)) return CompareStatus.CHANGE_ACCESS;
//        if (o.port != port) return CompareStatus.CHANGE_ACCESS;
//
//        if (o.username!=null) {
//            if (!o.username.equals(username)) return CompareStatus.CHANGE_ACCESS;
//        } else if (username!=null) return CompareStatus.CHANGE_ACCESS;
//
//        if (o.pwd!=null) {
//            if (!o.pwd.equals(pwd)) return CompareStatus.CHANGE_ACCESS;
//        } else if (pwd!=null) return CompareStatus.CHANGE_ACCESS;
//
//        if (o.localDC!=null) {
//            if (!o.localDC.equalsIgnoreCase(localDC)) return CompareStatus.CHANGE_ACCESS;
//        } else if (localDC!=null) return CompareStatus.CHANGE_ACCESS;
//
//        if (o.connectionTimeout != connectionTimeout) return CompareStatus.CHANGE_ACCESS;
//        if (o.requestTimeout != requestTimeout) return CompareStatus.CHANGE_ACCESS;
//        if (!o.consistencyLevel.equals(consistencyLevel)) return CompareStatus.CHANGE_ACCESS;
//        if (!o.table.equalsIgnoreCase(table)) return CompareStatus.CHANGE_ACCESS;
//
//        //  change in basic level (without Access level)
//        if (o.batch != batch) return CompareStatus.CHANGE;
//        if (o.dryRun != dryRun) return CompareStatus.CHANGE;
//
//        return CompareStatus.SAME;
//    }

}
