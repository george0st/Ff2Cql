package org.george0st;


import com.google.gson.Gson;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;

/**
 * The definition of Setup for CQL access (load setting from *.json file)
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
    public int bulk;
    public String table;

    private Setup(){
    }

    /**
     * Provide class instance for default connection file 'connection.json'
     * @return class instance
     */
    public static Setup getInstance() {
        return Setup.getInstance("connection.json");
    }

    /**
     * Provide class instance for specific connectionFile.
     * @param connectionFile JSON connection file
     * @return class instance
     */
    public static Setup getInstance(String connectionFile) {
        if (instances==null)
            instances=new HashMap<>();

        if (!instances.containsKey(connectionFile)) {
            try (FileReader fileReader = new FileReader(connectionFile)) {
                instances.put(connectionFile, (new Gson()).fromJson(fileReader, Setup.class));
            } catch (IOException ex) {
                instances.put(connectionFile, new Setup());
            }
        }
        return instances.get(connectionFile);
    }
}
