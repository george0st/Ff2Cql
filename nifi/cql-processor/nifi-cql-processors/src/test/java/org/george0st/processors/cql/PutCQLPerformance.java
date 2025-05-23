package org.george0st.processors.cql;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.george0st.processors.cql.helper.CqlTestSchema;
import org.george0st.processors.cql.helper.TestSetupWrite;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class PutCQLPerformance extends PutCQLBase {

    public PutCQLPerformance() throws IOException, InitializationException, InterruptedException {
        super();
    }

    // region SEQ Write

    @Test
    @DisplayName("SEQ Write - 100 items")
    void csvSequenceWrite100() throws Exception {
        String content=new CqlTestSchema().generateRndCSVString(100,true);
        MockFlowFile result;

        for (TestSetupWrite setup : setups) {
            result = runTest(setup, content);
            assertNotNull(result, String.format("Issue with processing in '%s'", setup.name));
            assertEquals(100, Long.parseLong(result.getAttribute(CQLAttributes.WRITE_COUNT)));
        }
    }

    @Test
    @DisplayName("SEQ Write - 1K items")
    void csvSequenceWrite1K() throws Exception {
        String content=new CqlTestSchema().generateRndCSVString(1_000,true);
        MockFlowFile result;

        for (TestSetupWrite setup : setups) {
            result = runTest(setup, content);
            assertNotNull(result, String.format("Issue with processing in '%s'", setup.name));
            assertEquals(1_000, Long.parseLong(result.getAttribute(CQLAttributes.WRITE_COUNT)));
        }
    }

    @Test
    @DisplayName("SEQ Write - 10K items")
    void csvSequenceWrite10K() throws Exception {
        String content=new CqlTestSchema().generateRndCSVString(10_000,true);
        MockFlowFile result;

        for (TestSetupWrite setup : setups) {
            result = runTest(setup, content);
            assertNotNull(result, String.format("Issue with processing in '%s'", setup.name));
            assertEquals(10_000, Long.parseLong(result.getAttribute(CQLAttributes.WRITE_COUNT)));
        }
    }

    @Test
    @DisplayName("SEQ Write - 30K items")
    void csvSequenceWrite30K() throws Exception {
        String content=new CqlTestSchema().generateRndCSVString(30_000,true);
        MockFlowFile result;

        for (TestSetupWrite setup : setups) {
            result = runTest(setup, content);
            assertNotNull(result, String.format("Issue with processing in '%s'", setup.name));
            assertEquals(30_000, Long.parseLong(result.getAttribute(CQLAttributes.WRITE_COUNT)));
        }
    }

    @Test
    @Disabled
    @DisplayName("SEQ Write - 100K items")
    void csvSequenceWrite100K() throws Exception {
        String content=new CqlTestSchema().generateRndCSVString(100_000,true);
        MockFlowFile result;

        for (TestSetupWrite setup : setups) {
            result = runTest(setup, content);
            assertNotNull(result, String.format("Issue with processing in '%s'", setup.name));
            assertEquals(100_000, Long.parseLong(result.getAttribute(CQLAttributes.WRITE_COUNT)));
        }
    }

    @Test
    @Disabled
    @DisplayName("SEQ Write - 150K items")
    void csvSequenceWrite150K() throws Exception {
        String content=new CqlTestSchema().generateRndCSVString(150_000,true);
        MockFlowFile result;

        for (TestSetupWrite setup : setups) {
            result = runTest(setup, content);
            assertNotNull(result, String.format("Issue with processing in '%s'", setup.name));
            assertEquals(150_000, Long.parseLong(result.getAttribute(CQLAttributes.WRITE_COUNT)));
        }
    }

    // endregion SEQ Write

    // region SEQ Write/Validate

    @Test
    @DisplayName("SEQ Write/Validate - 100 items")
    void csvSequenceWriteValidate100() throws Exception {
        String content=new CqlTestSchema().generateRndCSVString(100,true);
        MockFlowFile result;

        for (TestSetupWrite setup : setups) {
            result = runTest(setup, content, true);
            assertNotNull(result, String.format("Issue with processing in '%s'", setup.name));
            assertEquals(100, Long.parseLong(result.getAttribute(CQLAttributes.WRITE_COUNT)));
        }
    }

    @Test
    @DisplayName("SEQ Write/Validate - 1K items")
    void csvSequenceWriteValidate1K() throws Exception {
        String content=new CqlTestSchema().generateRndCSVString(1_000,true);
        MockFlowFile result;

        for (TestSetupWrite setup : setups) {
            result = runTest(setup, content, true);
            assertNotNull(result, String.format("Issue with processing in '%s'", setup.name));
            assertEquals(1_000, Long.parseLong(result.getAttribute(CQLAttributes.WRITE_COUNT)));
        }
    }

    @Test
    @DisplayName("SEQ Write/Validate - 10K items")
    void csvSequenceWriteValidate10K() throws Exception {
        String content=new CqlTestSchema().generateRndCSVString(10_000,true);
        MockFlowFile result;

        for (TestSetupWrite setup : setups) {
            result = runTest(setup, content, true);
            assertNotNull(result, String.format("Issue with processing in '%s'", setup.name));
            assertEquals(10_000, Long.parseLong(result.getAttribute(CQLAttributes.WRITE_COUNT)));
        }
    }

    @Test
    @DisplayName("SEQ Write/Validate - 30K items")
    void csvSequenceWriteValidate30K() throws Exception {
        String content=new CqlTestSchema().generateRndCSVString(30_000,true);
        MockFlowFile result;

        for (TestSetupWrite setup : setups) {
            result = runTest(setup, content, true);
            assertNotNull(result, String.format("Issue with processing in '%s'", setup.name));
            assertEquals(30_000, Long.parseLong(result.getAttribute(CQLAttributes.WRITE_COUNT)));
        }
    }

    @Test
    @Disabled
    @DisplayName("SEQ Write/Validate - 100K items")
    void csvSequenceWriteValidate100K() throws Exception {
        String content=new CqlTestSchema().generateRndCSVString(100_000,true);
        MockFlowFile result;

        for (TestSetupWrite setup : setups) {
            result = runTest(setup, content, true);
            assertNotNull(result, String.format("Issue with processing in '%s'", setup.name));
            assertEquals(100_000, Long.parseLong(result.getAttribute(CQLAttributes.WRITE_COUNT)));
        }
    }

    @Test
    @Disabled
    @DisplayName("SEQ Write/Validate - 150K items")
    void csvSequenceWriteValidate150K() throws Exception {
        String content=new CqlTestSchema().generateRndCSVString(150_000,true);
        MockFlowFile result;

        for (TestSetupWrite setup : setups) {
            result = runTest(setup, content, true);
            assertNotNull(result, String.format("Issue with processing in '%s'", setup.name));
            assertEquals(150_000, Long.parseLong(result.getAttribute(CQLAttributes.WRITE_COUNT)));
        }
    }

    // endregion SEQ Write/Validate

    // region RND Write

    @Test
    @DisplayName("RND Write - 100 items")
    void csvRandomWrite100() throws Exception {
        String content=new CqlTestSchema().generateRndCSVString(100,false);
        MockFlowFile result;

        for (TestSetupWrite setup : setups) {
            result = runTest(setup, content);
            assertNotNull(result, String.format("Issue with processing in '%s'", setup.name));
            assertEquals(100, Long.parseLong(result.getAttribute(CQLAttributes.WRITE_COUNT)));
        }
    }

    @Test
    @DisplayName("RND Write - 1K items")
    void csvRandomWrite1K() throws Exception {
        String content=new CqlTestSchema().generateRndCSVString(1_000,false);
        MockFlowFile result;

        for (TestSetupWrite setup : setups) {
            result = runTest(setup, content);
            assertNotNull(result, String.format("Issue with processing in '%s'", setup.name));
            assertEquals(1_000, Long.parseLong(result.getAttribute(CQLAttributes.WRITE_COUNT)));
        }
    }

    @Test
    @DisplayName("RND Write - 10K items")
    void csvRandomWrite10K() throws Exception {
        String content=new CqlTestSchema().generateRndCSVString(10_000,false);
        MockFlowFile result;

        for (TestSetupWrite setup : setups) {
            result = runTest(setup, content);
            assertNotNull(result, String.format("Issue with processing in '%s'", setup.name));
            assertEquals(10_000, Long.parseLong(result.getAttribute(CQLAttributes.WRITE_COUNT)));
        }
    }

    @Test
    @DisplayName("RND Write - 30K items")
    void csvRandomWrite30K() throws Exception {
        String content=new CqlTestSchema().generateRndCSVString(30_000,false);
        MockFlowFile result;

        for (TestSetupWrite setup : setups) {
            result = runTest(setup, content);
            assertNotNull(result, String.format("Issue with processing in '%s'", setup.name));
            assertEquals(30_000, Long.parseLong(result.getAttribute(CQLAttributes.WRITE_COUNT)));
        }
    }

    @Test
    @Disabled
    @DisplayName("RND Write - 100K items")
    void csvRandomWrite100K() throws Exception {
        String content=new CqlTestSchema().generateRndCSVString(100_000,false);
        MockFlowFile result;

        for (TestSetupWrite setup : setups) {
            result = runTest(setup, content);
            assertNotNull(result, String.format("Issue with processing in '%s'", setup.name));
            assertEquals(100_000, Long.parseLong(result.getAttribute(CQLAttributes.WRITE_COUNT)));
        }
    }

    @Test
    @Disabled
    @DisplayName("RND Write - 150K items")
    void csvRandomWrite150K() throws Exception {
        String content=new CqlTestSchema().generateRndCSVString(150_000,false);
        MockFlowFile result;

        for (TestSetupWrite setup : setups) {
            result = runTest(setup, content);
            assertNotNull(result, String.format("Issue with processing in '%s'", setup.name));
            assertEquals(150_000, Long.parseLong(result.getAttribute(CQLAttributes.WRITE_COUNT)));
        }
    }

    // endregion RND Write

}
