package org.george0st.processors.cql.helper;

import org.apache.nifi.processor.ProcessContext;
import org.george0st.processors.cql.CqlProcessor;

import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;


/**
 * The definition of Setup (load setting from *.json file)
 */
public class Setup {
    private static HashMap<String, Setup> instances;

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

    public void setBatch(long batch) { this.batch = batch; }
    public long getBatch() { return batch > 0 ? batch : 200 ; }

    public void setPwd(String pwd) { this.pwd = Base64.getEncoder().encodeToString(pwd.getBytes()); }
    public String getPwd() { return  new String(Base64.getDecoder().decode(this.pwd)); }

    public void setIPAddresses(String ipAddr) {
        String[] items = ipAddr.split(",");
        for (int i=0; i < items.length ; i++) items[i]=items[i].strip();
        this.ipAddresses = items;
    }
    public String getIPAddresses() { return String.join(",",this.ipAddresses); }


    public Setup(){
    }

    public Setup(ProcessContext context){
        setIPAddresses(context.getProperty(CqlProcessor.MY_IP_ADDRESSES.getName()).getValue());
        port=context.getProperty(CqlProcessor.MY_PORT.getName()).asInteger();
        username=context.getProperty(CqlProcessor.MY_USERNAME.getName()).getValue();
        setPwd(context.getProperty(CqlProcessor.MY_PASSWORD.getName()).getValue());
        localDC=context.getProperty(CqlProcessor.MY_LOCALDC.getName()).getValue();
        connectionTimeout=context.getProperty(CqlProcessor.MY_CONNECTION_TIMEOUT.getName()).asLong();
        requestTimeout=context.getProperty(CqlProcessor.MY_REQUEST_TIMEOUT.getName()).asLong();
        consistencyLevel=context.getProperty(CqlProcessor.MY_CONSISTENCY_LEVEL.getName()).getValue();
        table=context.getProperty(CqlProcessor.MY_TABLE.getName()).getValue();
        setBatch(context.getProperty(CqlProcessor.MY_BATCH_SIZE.getName()).asLong());
    }

    // Overriding equals() to compare two Setup objects
    @Override
    public boolean equals(Object o) {

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
        if (((Setup) o).batch != batch) return false;
        if (!((Setup) o).table.equalsIgnoreCase(table)) return false;

        return true;
    }

}
