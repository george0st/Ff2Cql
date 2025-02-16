package org.george0st.cql.helper;

import org.apache.nifi.controller.ConfigurationContext;
import org.george0st.cql.CQLControllerService;
import java.util.Arrays;


/**
 * The definition of ControllerSetup (load setting from *.json file)
 */
public class ControllerSetup {

    public String []ipAddresses;
    public int port;
    public String secureConnectionBundle;
    public String username;
    public String pwd;
    public String localDC;
    public long connectionTimeout;
    public long requestTimeout;
    public String consistencyLevel;

    public void setIPAddresses(String ipAddr) {
        String[] items = ipAddr.split(",");
        for (int i=0; i < items.length ; i++) items[i]=items[i].strip();
        this.ipAddresses = items;
    }
    public String getIPAddresses() { return String.join(",",this.ipAddresses); }

    public ControllerSetup(){
    }

    /**
     * Constructor with configuration context setting
     *
     * @param context   definition of process context
     */
    public ControllerSetup(final ConfigurationContext context){
        setIPAddresses(context.getProperty(CQLControllerService.IP_ADDRESSES).getValue());
        port=context.getProperty(CQLControllerService.PORT).asInteger();
        secureConnectionBundle=context.getProperty(CQLControllerService.SECURE_CONNECTION_BUNDLE).getValue();
        username=context.getProperty(CQLControllerService.USERNAME).getValue();
        pwd=context.getProperty(CQLControllerService.PASSWORD).getValue();
        localDC=context.getProperty(CQLControllerService.LOCAL_DC).getValue();
        connectionTimeout=context.getProperty(CQLControllerService.CONNECTION_TIMEOUT).asLong();
        requestTimeout=context.getProperty(CQLControllerService.REQUEST_TIMEOUT).asLong();
        consistencyLevel=context.getProperty(CQLControllerService.CONSISTENCY_LEVEL).getValue();
    }

    @Override
    public boolean equals(Object o) {

        //  check null
        if (o == null) return false;

        // If the object is compared with itself then return true
        if (o == this) return true;

        if (!(o instanceof ControllerSetup))
            return false;

        // IP and Port
        if (!Arrays.equals(((ControllerSetup)o).ipAddresses, ipAddresses)) return false;
        if (((ControllerSetup)o).port != port) return false;

        //  login
        if (!((ControllerSetup)o).secureConnectionBundle.equals(secureConnectionBundle)) return false;
        if (((ControllerSetup)o).username!=null) {
            if (!((ControllerSetup)o).username.equals(username)) return false;
        } else if (username!=null) return false;
        if (((ControllerSetup)o).pwd!=null) {
            if (!((ControllerSetup)o).pwd.equals(pwd)) return false;
        } else if (pwd!=null) return false;

        // access setting
        if (((ControllerSetup)o).localDC!=null) {
            if (!((ControllerSetup)o).localDC.equalsIgnoreCase(localDC)) return false;
        } else if (localDC!=null) return false;
        if (((ControllerSetup)o).connectionTimeout != connectionTimeout) return false;
        if (((ControllerSetup)o).requestTimeout != requestTimeout) return false;
        return ((ControllerSetup) o).consistencyLevel.equals(consistencyLevel);
    }

}
