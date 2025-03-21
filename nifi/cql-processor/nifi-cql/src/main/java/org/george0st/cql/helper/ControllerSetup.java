package org.george0st.cql.helper;

import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.ssl.SSLContextProvider;
import org.george0st.cql.CQLControllerService;
import java.util.Arrays;
import javax.net.ssl.SSLContext;


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
    public Object sslContext;

    public void setIPAddresses(String ipAddr) {
        if (ipAddr!=null) {
            String[] items = ipAddr.split(",");
            for (int i = 0; i < items.length; i++) items[i] = items[i].strip();
            this.ipAddresses = items;
        }
    }
    public String getIPAddresses() { return ipAddresses!=null ? String.join(",",this.ipAddresses) : null; }

    public ControllerSetup(){
    }

    /**
     * Constructor with configuration context setting
     *
     * @param context   definition of process context
     */
    public ControllerSetup(final ConfigurationContext context){
        setIPAddresses(context.getProperty(CQLControllerService.IP_ADDRESSES).evaluateAttributeExpressions().getValue());
        port=context.getProperty(CQLControllerService.PORT).evaluateAttributeExpressions().asInteger();
        secureConnectionBundle=context.getProperty(CQLControllerService.SECURE_CONNECTION_BUNDLE).evaluateAttributeExpressions().getValue();
        username=context.getProperty(CQLControllerService.USERNAME).evaluateAttributeExpressions().getValue();
        pwd=context.getProperty(CQLControllerService.PASSWORD).evaluateAttributeExpressions().getValue();
        localDC=context.getProperty(CQLControllerService.LOCAL_DC).evaluateAttributeExpressions().getValue();
        connectionTimeout=context.getProperty(CQLControllerService.CONNECTION_TIMEOUT).evaluateAttributeExpressions().asLong();
        requestTimeout=context.getProperty(CQLControllerService.REQUEST_TIMEOUT).evaluateAttributeExpressions().asLong();
        consistencyLevel=context.getProperty(CQLControllerService.CONSISTENCY_LEVEL).getValue();

        //  sslContext
        SSLContextProvider sslContextProvider = context.getProperty(CQLControllerService.SSL_CONTEXT_SERVICE).asControllerService(SSLContextProvider.class);
        sslContext = (SSLContext) sslContextProvider != null ? sslContextProvider.createContext() : null;
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
