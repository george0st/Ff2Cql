package org.george0st.processors.cql.helper;

import org.apache.nifi.processor.ProcessContext;
import org.george0st.processors.cql.PutCQL;
import java.util.Arrays;


/**
 * The definition of Setup (load setting from *.json file)
 */
public class Setup {
    public enum CompareStatus {
        SAME,
        CHANGE,
        CHANGE_ACCESS;
    }

    public String []ipAddresses;
    public int port;
    public String username;
    public String pwd;
    public String localDC;
    public long connectionTimeout;
    public long requestTimeout;
    public String consistencyLevel;
    private long batch;
    public String table;
    public boolean dryRun;

    public void setBatch(long batch) { this.batch = batch; }
    public long getBatch() { return batch > 0 ? batch : 200 ; }

    public void setIPAddresses(String ipAddr) {
        String[] items = ipAddr.split(",");
        for (int i=0; i < items.length ; i++) items[i]=items[i].strip();
        this.ipAddresses = items;
    }
    public String getIPAddresses() { return String.join(",",this.ipAddresses); }

    public Setup(){
    }

    /**
     * Constructor with process context setting
     *
     * @param context   definition of process context
     */
    public Setup(ProcessContext context){
        setIPAddresses(context.getProperty(PutCQL.MY_IP_ADDRESSES).getValue());
        port=context.getProperty(PutCQL.MY_PORT).asInteger();
        username=context.getProperty(PutCQL.MY_USERNAME).getValue();
        pwd=context.getProperty(PutCQL.MY_PASSWORD).getValue();
        localDC=context.getProperty(PutCQL.MY_LOCALDC).getValue();
        connectionTimeout=context.getProperty(PutCQL.MY_CONNECTION_TIMEOUT).asLong();
        requestTimeout=context.getProperty(PutCQL.MY_REQUEST_TIMEOUT).asLong();
        consistencyLevel=context.getProperty(PutCQL.MY_CONSISTENCY_LEVEL).getValue();
        table=context.getProperty(PutCQL.MY_TABLE).getValue();
        setBatch(context.getProperty(PutCQL.MY_BATCH_SIZE).asLong());
        dryRun=context.getProperty(PutCQL.MY_DRY_RUN).asBoolean();
    }

    public CompareStatus compare(Setup o){

        //  check null
        if (o == null) return CompareStatus.CHANGE_ACCESS;

        // If the object is compared with itself then return true
        if (o == this) return CompareStatus.SAME;

        //  change in Access level
        if (!Arrays.equals(o.ipAddresses, ipAddresses)) return CompareStatus.CHANGE_ACCESS;
        if (o.port != port) return CompareStatus.CHANGE_ACCESS;

        if (o.username!=null) {
            if (!o.username.equals(username)) return CompareStatus.CHANGE_ACCESS;
        } else if (username!=null) return CompareStatus.CHANGE_ACCESS;

        if (o.pwd!=null) {
            if (!o.pwd.equals(pwd)) return CompareStatus.CHANGE_ACCESS;
        } else if (pwd!=null) return CompareStatus.CHANGE_ACCESS;

        if (o.localDC!=null) {
            if (!o.localDC.equalsIgnoreCase(localDC)) return CompareStatus.CHANGE_ACCESS;
        } else if (localDC!=null) return CompareStatus.CHANGE_ACCESS;

        if (o.connectionTimeout != connectionTimeout) return CompareStatus.CHANGE_ACCESS;
        if (o.requestTimeout != requestTimeout) return CompareStatus.CHANGE_ACCESS;
        if (!o.consistencyLevel.equals(consistencyLevel)) return CompareStatus.CHANGE_ACCESS;
        if (!o.table.equalsIgnoreCase(table)) return CompareStatus.CHANGE_ACCESS;

        //  change in basic level (without Access level)
        if (o.batch != batch) return CompareStatus.CHANGE;
        if (o.dryRun != dryRun) return CompareStatus.CHANGE;

        return CompareStatus.SAME;
    }

}
