package org.george0st.processors.cql.helper;

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

    public Setup(){
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
