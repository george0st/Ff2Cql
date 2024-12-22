package org.george0st;

import com.opencsv.exceptions.CsvValidationException;

import java.io.IOException;

interface CqlProcessor {
    public void execute(String fileName) throws CsvValidationException, IOException;
}
