package org.george0st.processors.cql;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.george0st.processors.cql.helper.CqlTestSchema;
import org.george0st.processors.cql.helper.TestSetupRead;
import org.george0st.processors.cql.helper.TestSetupWrite;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class GetCQLPerformance extends GetCQLBase {

    public GetCQLPerformance() throws Exception {
        super(true);
    }

    @Test
    public void testRead10K() throws Exception {
        MockFlowFile result;

        // Prepare data for performance test
        PutCQLPerformance putCQL = new PutCQLPerformance();
        putCQL.init();
        putCQL.csvRandomWrite10K();

        //  sleep before read (2 seconds)
        Thread.sleep(2000);

        // Read data
        for (TestSetupRead setup : setups) {
            setup.columnNames="colbigint, colint";

            result = runTest(setup);

            //  check amount of read items
            assertNotNull(result, String.format("Issue with processing in '%s'", setup.name));
            assertEquals(10_000, Long.parseLong(result.getAttribute(CQLAttributes.READ_COUNT)));
        }
    }


    @Test
    public void testRead30K() throws Exception {
        MockFlowFile result;

        // Prepare data for performance test
        PutCQLPerformance putCQL = new PutCQLPerformance();
        putCQL.init();
        putCQL.csvRandomWrite30K();

        //  sleep before read (2 seconds)
        Thread.sleep(2000);

        // Read data
        for (TestSetupRead setup : setups) {
            setup.columnNames="colbigint, colint";

            result = runTest(setup);

            //  check amount of read items
            assertNotNull(result, String.format("Issue with processing in '%s'", setup.name));
            assertEquals(30_000, Long.parseLong(result.getAttribute(CQLAttributes.READ_COUNT)));
        }
    }

    @Test
    public void testRead100K() throws Exception {
        MockFlowFile result;

        // Prepare data for performance test
        PutCQLPerformance putCQL = new PutCQLPerformance();
        putCQL.init();
        putCQL.csvRandomWrite100K();

        //  sleep before read (2 seconds)
        Thread.sleep(2000);

        // Read data
        for (TestSetupRead setup : setups) {
            setup.columnNames="colbigint, colint";

            result = runTest(setup);

            //  check amount of read items
            assertNotNull(result, String.format("Issue with processing in '%s'", setup.name));
            assertEquals(100_000, Long.parseLong(result.getAttribute(CQLAttributes.READ_COUNT)));
        }
    }

    @Test
    public void testRead150K() throws Exception {
        MockFlowFile result;

        // Prepare data for performance test
        PutCQLPerformance putCQL = new PutCQLPerformance();
        putCQL.init();
        putCQL.csvRandomWrite150K();

        //  sleep before read (2 seconds)
        Thread.sleep(2000);

        // Read data
        for (TestSetupRead setup : setups) {
            setup.columnNames="colbigint, colint";

            result = runTest(setup);

            //  check amount of read items
            assertNotNull(result, String.format("Issue with processing in '%s'", setup.name));
            assertEquals(150_000, Long.parseLong(result.getAttribute(CQLAttributes.READ_COUNT)));
        }
    }

}
