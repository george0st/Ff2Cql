package org.george0st.processors.cql.helper;

import com.google.gson.Gson;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.util.TestRunner;
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
     * Choose the first existing controllerSetup file name.
     * @param path Path for controllerSetup file location.
     * @param files List of controllerSetup file names for check.
     * @return ControllerSetup file name.
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
//        setProperty(CQLClientService.IP_ADDRESSES, String.join(",", ipAddresses));
//        setProperty(PutCQL.MY_PORT, String.valueOf(port));
//        setProperty(PutCQL.MY_LOCALDC, localDC);
//        setProperty(PutCQL.MY_USERNAME, username);
//        setProperty(PutCQL.MY_PASSWORD, pwd);
//        setProperty(PutCQL.MY_CONNECTION_TIMEOUT, String.valueOf(connectionTimeout));
//        setProperty(PutCQL.MY_REQUEST_TIMEOUT, String.valueOf(requestTimeout));
        setProperty(PutCQL.WRITE_CONSISTENCY_LEVEL, writeConsistencyLevel);
        setProperty(PutCQL.BATCH_SIZE, String.valueOf(getBatchSize()));
        setProperty(PutCQL.TABLE, table);
        setProperty(PutCQL.BATCH_SIZE, String.valueOf(dryRun));
    }
}
