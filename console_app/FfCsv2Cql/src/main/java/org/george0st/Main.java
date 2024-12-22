package org.george0st;


import com.opencsv.exceptions.CsvValidationException;
import org.george0st.helper.Setup;
import org.george0st.processor.CsvCqlProcessor;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws CsvValidationException, IOException {
        String setupFile= Setup.getSetupFile(new String[]{"connection-private.json","connection.json"});
        CsvCqlProcessor aa = new CsvCqlProcessor(Setup.getInstance(setupFile));
        aa.execute("test.csv");

        System.out.printf("! DONE !");
    }
}