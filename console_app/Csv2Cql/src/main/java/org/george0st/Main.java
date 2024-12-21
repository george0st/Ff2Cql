package org.george0st;


import java.io.File;

public class Main {

    public static void main(String[] args) {
        String setupFile=Setup.getSetupFile(new String[]{"connection-private.json","connection.json"});
        CsvCqlProcessor aa = new CsvCqlProcessor(Setup.getInstance(setupFile));
        aa.execute("test.csv");

        System.out.printf("! DONE !");
    }
}