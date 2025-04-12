package org.george0st.processors.cql;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.george0st.processors.cql.helper.CqlTestSchema;
import org.george0st.processors.cql.helper.TestSetupRead;
import org.george0st.processors.cql.helper.TestSetupWrite;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

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

    @Test
    public void testRead10KMultiple() throws Exception {
        String content = "\"colbigint\",\"colint\",\"coltext\",\"colfloat\",\"coldouble\",\"coldate\",\"coltime\",\"coltimestamp\",\"colboolean\",\"coluuid\",\"colsmallint\",\"coltinyint\",\"coltimeuuid\",\"colvarchar\"\n" +
                "\"0\",\"1064\",\"zeVOKGnORq\",\"627.6811\",\"395.8522407512559\",\"1971-11-12\",\"03:37:15\",\"2000-09-25T22:18:45Z\",\"false\",\"6080071f-4dd1-4ea5-b711-9ad0716e242a\",\"8966\",\"55\",\"f45e58f5-c3b7-11ef-8d19-97ae87be7c54\",\"Tzxsw\"\n" +
                "\"1\",\"1709\",\"7By0z5QEXh\",\"652.03955\",\"326.9081263857284\",\"2013-12-17\",\"08:43:09\",\"2010-04-27T07:02:27Z\",\"false\",\"7d511666-2f81-41c4-9d5c-a5fa87f7d1c3\",\"24399\",\"38\",\"f45e8006-c3b7-11ef-8d19-172ff8d0d752\",\"exAbN\"\n";
        List<MockFlowFile> results;

        // Prepare data for performance test
        PutCQLPerformance putCQL = new PutCQLPerformance();
        putCQL.init();
        putCQL.csvRandomWrite10K();

        //  sleep before read (2 seconds)
        Thread.sleep(2000);

        // Read data
        for (TestSetupRead setup : setups) {
            setup.columnNames="colbigint, colint";

            //results = runTestParallel(setup, content, 5);
            results = runTestParallel(setup, 5);

            //  check amount of read items
            for (MockFlowFile result: results) {
                assertNotNull(result, String.format("Issue with processing in '%s'", setup.name));
                assertEquals(10_000, Long.parseLong(result.getAttribute(CQLAttributes.READ_COUNT)));
            }
        }
    }
}
