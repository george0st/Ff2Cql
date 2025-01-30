package org.george0st.processors.cql;

import com.google.gson.Gson;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class TestProperty {

    private static HashMap<String, TestProperty> instances;

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

    private TestProperty(){

    }

    public static TestProperty getInstance() throws IOException {
        return TestProperty.getInstance("test-properties.json");
    }

    public static TestProperty getInstance(String propertyFile) throws IOException {
        if (instances==null)
            instances=new HashMap<>();

        if (!instances.containsKey(propertyFile)) {
            try (FileReader fileReader = new FileReader(propertyFile)) {
                instances.put(propertyFile, (new Gson()).fromJson(fileReader, TestProperty.class));
            }
        }
        return instances.get(propertyFile);
    }

    /**
     * Choose the first existing setup file name.
     * @param path Path for setup file location.
     * @param files List of setup file names for check.
     * @return Setup file name.
     */
    public static String getTestPropertyFile(String path, String[] files){
        String fileName;

        for (String file: files) {
            fileName = String.format("%s/%s", path, file);
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
}
