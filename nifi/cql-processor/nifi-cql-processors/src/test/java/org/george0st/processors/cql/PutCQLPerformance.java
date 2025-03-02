package org.george0st.processors.cql;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.reporting.InitializationException;
import org.george0st.processors.cql.helper.CqlCreateSchema;
import org.george0st.processors.cql.helper.TestSetup;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class PutCQLPerformance extends PutCQLBase {

    public PutCQLPerformance() throws IOException, InitializationException, InterruptedException {
        super();
    }

    @Test
    @DisplayName("Seq W, 1. 100 items")
    void csvWRSequence100() throws Exception {
        String content=new CqlCreateSchema().generateRndCSVString(100,true);
        FlowFile result;

        for (TestSetup setup : setups) {
            result = coreTest(setup, content);
            assertNotNull(result, String.format("Issue with processing in '%s'", setup.name));
            assertEquals(100, Long.parseLong(result.getAttribute(PutCQL.ATTRIBUTE_COUNT)));
        }
    }

    @Test
    @DisplayName("Seq W, 2. 1K items")
    void csvWRSequence1K() throws Exception {
        String content=new CqlCreateSchema().generateRndCSVString(1_000,true);
        FlowFile result;

        for (TestSetup setup : setups) {
            result = coreTest(setup, content);
            assertNotNull(result, String.format("Issue with processing in '%s'", setup.name));
            assertEquals(1_000, Long.parseLong(result.getAttribute(PutCQL.ATTRIBUTE_COUNT)));
        }
    }

    @Test
    @DisplayName("Seq W, 2. 10K items")
    void csvWRSequence10K() throws Exception {
        String content=new CqlCreateSchema().generateRndCSVString(10_000,true);
        FlowFile result;

        for (TestSetup setup : setups) {
            result = coreTest(setup, content);
            assertNotNull(result, String.format("Issue with processing in '%s'", setup.name));
            assertEquals(10_000, Long.parseLong(result.getAttribute(PutCQL.ATTRIBUTE_COUNT)));
        }
    }

    @Test
    @DisplayName("Seq W, 3. 100K items")
    void csvWRSequence100K() throws Exception {
        String content=new CqlCreateSchema().generateRndCSVString(100_000,true);
        FlowFile result;

        for (TestSetup setup : setups) {
            result = coreTest(setup, content);
            assertNotNull(result, String.format("Issue with processing in '%s'", setup.name));
            assertEquals(100_000, Long.parseLong(result.getAttribute(PutCQL.ATTRIBUTE_COUNT)));
        }
    }

}
