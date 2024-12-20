package org.george0st;


import java.io.File;

public class Main {

    private static String setupFile(String[] files){
        for (String file: files)
            if (new File(file).exists())
                return file;
        return null;
    }

    public static void main(String[] args) {

        String setupFile=Main.setupFile(new String[]{"connection-private.json","connection.json"});
        CsvCqlProcessor aa = new CsvCqlProcessor(Setup.getInstance(setupFile));
        aa.execute("test.csv");

        System.out.printf("Hello and welcome!");
    }
}