package org.george0st.processors.cql.helper;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.util.TestRunner;
import org.george0st.cql.CQLControllerService;
import org.george0st.processors.cql.GetCQL;
import org.george0st.processors.cql.PutCQL;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class TestSetupRead extends SetupRead {
    public boolean enable;
    public String name;
    public String []ipAddresses;
    public int port;
    public String secureConnectionBundle;
    public String username;
    public String pwd;
    public String localDC;
    public long connectionTimeout;
    public long requestTimeout;
    public String defaultConsistencyLevel;
    public String replication;
    public String compaction;

    private TestSetupRead(){
    }

    public void setIPAddresses(String ipAddr) {
        if (ipAddr!=null) {
            String[] items = ipAddr.split(",");
            for (int i = 0; i < items.length; i++) items[i] = items[i].strip();
            this.ipAddresses = items;
        }
    }
    public String getIPAddresses() { return String.join(",",this.ipAddresses); }

    public static TestSetupRead getInstance(String propertyFile) throws IOException {
        try (FileReader fileReader = new FileReader(propertyFile)) {
            TestSetupRead setup = (new Gson()).fromJson(fileReader, TestSetupRead.class);

            if (!setup.enable) return null;

            //  default setting
            if (setup.compaction == null)
                setup.compaction = "{'class':'SizeTieredCompactionStrategy'}";
            if (setup.replication == null)
                setup.replication = "{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}";
            return setup;
        }
    }

    /**
     * Choose the first existing controllerSetup file name.
     * @param path Path for controllerSetup file location.
     * @param files List of controllerSetup file names for check.
     * @return ControllerSetup file name.
     */
    public static String getTestPropertyFile(String path, String[] files){
        String fileName;

        for (String file: files) {
            fileName = path.endsWith("/") ? String.format("%s%s", path, file) : String.format("%s/%s", path, file);
            if (new File(fileName).exists())
                return fileName;
        }
        return null;
    }

    public static String getTestPropertyFile(String[] files){
        for (String file: files)
            if (new File(file).exists())
                return file;
        return null;
    }

    public void setControllerProperty(TestRunner testRunner, CQLControllerService testService, PropertyDescriptor property, String propertyValue) {
        //if (propertyValue != null)
        testRunner.setProperty(testService, property, propertyValue);
    }

    public void setProperty(TestRunner testRunner, PropertyDescriptor property, String propertyValue) {
        if (propertyValue != null)
            testRunner.setProperty(property, propertyValue);
    }

    private String getJson(String item) {
        if (item!=null) {
            try {
                if (item.toLowerCase().endsWith(".json")) {
                    String[] items = item.split(",");
                    File jsonFile = new File(items[1].strip());
                    return jsonFile.isFile() ?
                            JsonParser.parseReader(new FileReader(jsonFile)).getAsJsonObject().get(items[0].strip()).getAsString() :
                            item;
                }
            }
            catch(Exception ex) {
            }
        }
        return item;
    }

    /**
     * Setting test runner based on test setting
     */
    public void setProperty(TestRunner testRunner, CQLControllerService testService) {
        // clear all properties before the setting
        testRunner.clearProperties();

        //  set CONTROLLER properties
        setControllerProperty(testRunner, testService, CQLControllerService.IP_ADDRESSES, ipAddresses!=null ? String.join(",", ipAddresses) : (String)null);
        setControllerProperty(testRunner, testService, CQLControllerService.PORT, String.valueOf(port));
        setControllerProperty(testRunner, testService, CQLControllerService.SECURE_CONNECTION_BUNDLE, secureConnectionBundle);
        setControllerProperty(testRunner, testService, CQLControllerService.USERNAME, getJson(username));
        setControllerProperty(testRunner, testService, CQLControllerService.PASSWORD, getJson(pwd));
        setControllerProperty(testRunner, testService, CQLControllerService.LOCAL_DC, localDC);
        setControllerProperty(testRunner, testService, CQLControllerService.CONNECTION_TIMEOUT, String.valueOf(connectionTimeout));
        setControllerProperty(testRunner, testService, CQLControllerService.REQUEST_TIMEOUT, String.valueOf(requestTimeout));
        setControllerProperty(testRunner, testService, CQLControllerService.DEFAULT_CONSISTENCY_LEVEL, defaultConsistencyLevel);

        //  set PROCESSOR properties
        setProperty(testRunner, GetCQL.SERVICE_CONTROLLER, GetCQL.SERVICE_CONTROLLER.getName());
        setProperty(testRunner, GetCQL.COLUMN_NAMES, columnNames);
        setProperty(testRunner, GetCQL.WHERE_CLAUSE, whereClause);
        setProperty(testRunner, GetCQL.CQL_QUERY, cqlQuery);
        setProperty(testRunner, GetCQL.TABLE, table);
        setProperty(testRunner, GetCQL.CONSISTENCY_LEVEL, consistencyLevel);

        testRunner.setValidateExpressionUsage(false);
    }
}
