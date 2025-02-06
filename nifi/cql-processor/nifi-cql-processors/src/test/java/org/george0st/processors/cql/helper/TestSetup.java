package org.george0st.processors.cql.helper;

import com.google.gson.Gson;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.george0st.processors.cql.PutCQL;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class TestSetup extends Setup {

    public String name;
    public String compaction;

    private TestRunner testRunner;

    private TestSetup(){
    }

    private void setRunner(TestRunner testRunner){
        this.testRunner=testRunner;
    }
    public static TestSetup getInstance(TestRunner testRunner, String propertyFile) throws IOException {
        try (FileReader fileReader = new FileReader(propertyFile)) {
            TestSetup setup = (new Gson()).fromJson(fileReader, TestSetup.class);

            //  default setting
            if (setup.compaction==null)
                setup.compaction="{'class':'SizeTieredCompactionStrategy'}";
            setup.setRunner(testRunner);

            return setup;
        }
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

    public void setProperty(PropertyDescriptor property, String propertyValue) {
        if (propertyValue != null)
            testRunner.setProperty(property, propertyValue);
    }

    /**
     * Setting test runner based on test setting
     */
    public void setProperty(){
        setProperty(PutCQL.MY_IP_ADDRESSES, String.join(",", ipAddresses));
        setProperty(PutCQL.MY_PORT, String.valueOf(port));
        setProperty(PutCQL.MY_LOCALDC, localDC);
        setProperty(PutCQL.MY_USERNAME, username);
        setProperty(PutCQL.MY_PASSWORD, pwd);
        setProperty(PutCQL.MY_CONNECTION_TIMEOUT, String.valueOf(connectionTimeout));
        setProperty(PutCQL.MY_REQUEST_TIMEOUT, String.valueOf(requestTimeout));
        setProperty(PutCQL.MY_CONSISTENCY_LEVEL, consistencyLevel);
        setProperty(PutCQL.MY_BATCH_SIZE, String.valueOf(getBatch()));
        setProperty(PutCQL.MY_TABLE, table);
        setProperty(PutCQL.MY_BATCH_SIZE, String.valueOf(dryRun));
    }
}
