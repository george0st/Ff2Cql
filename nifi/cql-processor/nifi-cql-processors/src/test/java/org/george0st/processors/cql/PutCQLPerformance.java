package org.george0st.processors.cql;

import org.apache.nifi.reporting.InitializationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

public class PutCQLPerformance extends PutCQLBase {

    public PutCQLPerformance() throws IOException, InitializationException, InterruptedException {
        super();

    }

    @Test
    @DisplayName("Sequence WR, 1. 100 items in CSV")
    void csvWRSequence100() throws Exception {
//        File randomFile=schema.generateRndCSVFile(100, true);
//        coreTest(randomFile, true);
    }

}
