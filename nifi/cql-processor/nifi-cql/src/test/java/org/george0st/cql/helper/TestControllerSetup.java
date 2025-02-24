package org.george0st.cql.helper;

import com.datastax.oss.driver.internal.core.addresstranslation.PassThroughAddressTranslator;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.util.TestRunner;
import org.george0st.cql.CQLControllerService;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class TestControllerSetup extends ControllerSetup{
    public boolean enable;
    public String name;

    private TestRunner testRunner;
    private CQLControllerService testService;

    private TestControllerSetup(){
    }

    private void setRunner(TestRunner testRunner, CQLControllerService service){
        this.testRunner=testRunner;
        this.testService =service;
    }

    public static TestControllerSetup getInstance(TestRunner runner, CQLControllerService service, String propertyFile) throws IOException {
        try (FileReader fileReader = new FileReader(propertyFile)) {
            TestControllerSetup setup = (new Gson()).fromJson(fileReader, TestControllerSetup.class);

            if (!setup.enable) return null;
            setup.testRunner = runner;
            setup.testService = service;
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

    public void setProperty(PropertyDescriptor property, String propertyValue) {
        //if (propertyValue != null)
        testRunner.setProperty(testService, property, propertyValue);
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
    public void setProperty() {
        // clear all properties before the setting
        testRunner.clearProperties();

        setProperty(CQLControllerService.IP_ADDRESSES, ipAddresses!=null ? String.join(",", ipAddresses) : (String)null);
        setProperty(CQLControllerService.PORT, String.valueOf(port));
        setProperty(CQLControllerService.SECURE_CONNECTION_BUNDLE, secureConnectionBundle);
        setProperty(CQLControllerService.USERNAME, getJson(username));
        setProperty(CQLControllerService.PASSWORD, getJson(pwd));
        setProperty(CQLControllerService.LOCAL_DC, localDC);
        setProperty(CQLControllerService.CONNECTION_TIMEOUT, String.valueOf(connectionTimeout));
        setProperty(CQLControllerService.REQUEST_TIMEOUT, String.valueOf(requestTimeout));
        setProperty(CQLControllerService.CONSISTENCY_LEVEL, String.valueOf(consistencyLevel));
    }

}
