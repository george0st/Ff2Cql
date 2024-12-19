package org.george0st;


public class Main {
    public static void main(String[] args) {


        CsvCqlProcessor aa = new CsvCqlProcessor(Setup.getInstance("connection.json"));
        aa.execute("test.csv");
        //aa.Connect();

        //aa.Test();
//
        System.out.printf("Hello and welcome!");
    }
}