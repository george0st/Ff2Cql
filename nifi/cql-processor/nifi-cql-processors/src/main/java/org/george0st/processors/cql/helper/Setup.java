package org.george0st.processors.cql.helper;

//import com.google.gson.Gson;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
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
    private String pwd;
    public String localDC;
    public long connectionTimeout;
    public long requestTimeout;
    public String consistencyLevel;
    private long bulk;
    public String table;

    public void setBulk(long bulk) { this.bulk = bulk; }
    public long getBulk() { return bulk > 0 ? bulk : 200 ; }

    public void setPwd(String pwd) { this.pwd = Base64.getEncoder().encodeToString(pwd.getBytes()); }
    public String getPwd() { return  new String(Base64.getDecoder().decode(this.pwd)); }

    public Setup(){
    }

    // Overriding equals() to compare two Setup objects
    @Override
    public boolean equals(Object o) {

        // If the object is compared with itself then return true
        if (o == this) {
            return true;
        }

        /* Check if o is an instance of Setup or not
          "null instanceof [type]" also returns false */
        if (!(o instanceof Setup)) {
            return false;
        }

        //  TODO: compare

        return true;
    }

}
