package org.george0st.processors.cql.helper;

import org.apache.nifi.processor.ProcessContext;
import org.george0st.processors.cql.PutCql;

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
        setIPAddresses(context.getProperty(PutCql.MY_IP_ADDRESSES.getName()).getValue());
        port=context.getProperty(PutCql.MY_PORT.getName()).asInteger();
        username=context.getProperty(PutCql.MY_USERNAME.getName()).getValue();
        pwd=context.getProperty(PutCql.MY_PASSWORD.getName()).getValue();
        localDC=context.getProperty(PutCql.MY_LOCALDC.getName()).getValue();
        connectionTimeout=context.getProperty(PutCql.MY_CONNECTION_TIMEOUT.getName()).asLong();
        requestTimeout=context.getProperty(PutCql.MY_REQUEST_TIMEOUT.getName()).asLong();
        consistencyLevel=context.getProperty(PutCql.MY_CONSISTENCY_LEVEL.getName()).getValue();
        table=context.getProperty(PutCql.MY_TABLE.getName()).getValue();
        setBatch(context.getProperty(PutCql.MY_BATCH_SIZE.getName()).asLong());
        dryRun=context.getProperty(PutCql.MY_DRY_RUN.getName()).asBoolean();
    }

    public CompareStatus compare(Setup o){

        //  check null
        if (o == null) return CompareStatus.CHANGE_ACCESS;

        // If the object is compared with itself then return true
        if (o == this) return CompareStatus.SAME;

        //  change in Access level
        if (!Arrays.equals(o.ipAddresses, ipAddresses)) return CompareStatus.CHANGE_ACCESS;
        if (o.port != port) return CompareStatus.CHANGE_ACCESS;
        if (!o.username.equals(username)) return CompareStatus.CHANGE_ACCESS;
        if (!o.pwd.equals(pwd)) return CompareStatus.CHANGE_ACCESS;
        if (!o.localDC.equalsIgnoreCase(localDC)) return CompareStatus.CHANGE_ACCESS;
        if (o.connectionTimeout != connectionTimeout) return CompareStatus.CHANGE_ACCESS;
        if (o.requestTimeout != requestTimeout) return CompareStatus.CHANGE_ACCESS;
        if (!o.consistencyLevel.equals(consistencyLevel)) return CompareStatus.CHANGE_ACCESS;
        if (!o.table.equalsIgnoreCase(table)) return CompareStatus.CHANGE_ACCESS;

        //  change in basic level (without Access level)
        if (o.batch != batch) return CompareStatus.CHANGE;
        if (o.dryRun != dryRun) return CompareStatus.CHANGE;

        return CompareStatus.SAME;
    }

    // Overriding equals() to compare two Setup objects
    @Override
    public boolean equals(Object o) {

        // check null
        if (o == null) return false;

        // If the object is compared with itself then return true
        if (o == this) return true;

        // Check if o is an instance of Setup or not "null instanceof [type]" also returns false
        if (!(o instanceof Setup)) return false;

        //  own compare
        if (!Arrays.equals(((Setup) o).ipAddresses, ipAddresses)) return false;
        if (((Setup) o).port != port) return false;
        if (!((Setup) o).username.equals(username)) return false;
        if (!((Setup) o).pwd.equals(pwd)) return false;
        if (!((Setup) o).localDC.equalsIgnoreCase(localDC)) return false;
        if (((Setup) o).connectionTimeout != connectionTimeout) return false;
        if (((Setup) o).requestTimeout != requestTimeout) return false;
        if (!((Setup) o).consistencyLevel.equals(consistencyLevel)) return false;
        if (!((Setup) o).table.equalsIgnoreCase(table)) return false;

        if (((Setup)o).batch != batch) return false;
        if (((Setup)o).dryRun != dryRun) return false;

        return true;
    }

}
