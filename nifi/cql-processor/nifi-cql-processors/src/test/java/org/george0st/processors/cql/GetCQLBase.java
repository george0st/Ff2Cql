package org.george0st.processors.cql;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.george0st.cql.CQLControllerService;
import org.george0st.processors.cql.helper.ReadableValue;
import org.george0st.processors.cql.helper.TestSetupRead;
import org.george0st.processors.cql.helper.TestSetupWrite;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetCQLBase {

    protected TestRunner testRunner;
    protected CQLControllerService testService;
    protected List<TestSetupRead> setups;

    public GetCQLBase(boolean performanceTest) throws Exception {
        //  create TestSetupWrite scope based on test-*.json files
        setups = createSetup();

        if (!performanceTest){
            //  Prepare data for reading (including build schema, if not exist)
            PutCQLFunction putCQL = new PutCQLFunction();
            putCQL.init();
            putCQL.testBasic();
        }
    }

    protected ArrayList<TestSetupRead> createSetup() throws IOException {
        ArrayList<TestSetupRead> setup = new ArrayList<TestSetupRead>();

        addTestScope(setup,
                TestSetupWrite.getTestPropertyFile("./src/test",
                        new String[]{"test-get-cassandra.json", "test-properties.json"}));
        addTestScope(setup,
                TestSetupWrite.getTestPropertyFile("./src/test",
                        new String[]{"test-get-scylla.json", "test-properties.json"}));
        addTestScope(setup,
                TestSetupWrite.getTestPropertyFile("./src/test",
                        new String[]{"test-get-astra.json", "test-properties.json"}));
        return setup;
    }

    protected void addTestScope(List<TestSetupRead> setup, String propertyFile) throws IOException {
        TestSetupRead itm;

        itm = TestSetupRead.getInstance(propertyFile);
        if (itm!=null) setup.add(itm);
    }

    @BeforeEach
    public void init() throws IOException, InterruptedException, InitializationException {
        testRunner = TestRunners.newTestRunner(GetCQL.class);
        testService = new CQLControllerService();
        testRunner.addControllerService(PutCQL.SERVICE_CONTROLLER.getName(), testService);

        for (TestSetupRead setup: setups) {
            System.out.println(String.format("Test scope: '%s'", setup.name));
        }
    }

    protected MockFlowFile runTest(TestSetupRead setup) throws Exception {
        return runTestWithProperty(setup, null, null, null, false);
    }

    protected List<MockFlowFile> runTestParallel(TestSetupRead setup, int parallel) throws Exception {
        return runTestParallelWithProperty(setup, null, null, null, false, parallel);
    }

    protected MockFlowFile runTest(TestSetupRead setup, String content) throws Exception {
        return runTestWithProperty(setup, content, null, null, false);
    }

    protected List<MockFlowFile> runTestParallel(TestSetupRead setup, String content, int parallel) throws Exception {
        return runTestParallelWithProperty(setup, content, null, null, false, parallel);
    }

    protected MockFlowFile runTest(TestSetupRead setup, String content, boolean validate) throws Exception {
        return runTestWithProperty(setup, content, null, null, validate);
    }

    protected MockFlowFile runTestWithProperty(TestSetupRead setup, String content, PropertyDescriptor property, String propertyValue) throws Exception {
        return  runTestWithProperty(setup, content, property, propertyValue, false);
    }

    protected MockFlowFile runTestWithProperty(TestSetupRead setup, String content, PropertyDescriptor property, String propertyValue, boolean validate) throws Exception {
        MockFlowFile result;

        if (content!=null)
            testRunner.enqueue(content);
        setup.setProperty(testRunner, testService);
        if (property != null)
            setup.setProperty(testRunner, property, propertyValue);
        testRunner.enableControllerService(testService);
        result = coreTest(setup, content, validate);
        testRunner.disableControllerService(testService);
        return result;
    }

    protected List<MockFlowFile> runTestParallelWithProperty(TestSetupRead setup, String content, PropertyDescriptor property, String propertyValue, boolean validate, int parallel) throws Exception {
        List<MockFlowFile> results;

        if (content!=null)
            for (int i=0;i<parallel;i++)
                testRunner.enqueue(content);
        setup.setProperty(testRunner, testService);
        if (property != null)
            setup.setProperty(testRunner, property, propertyValue);
        testRunner.enableControllerService(testService);
        if (parallel>1)
            testRunner.setThreadCount(parallel);
        results = coreTestParallel(setup, content, validate, parallel);
        testRunner.disableControllerService(testService);
        return results;
    }

    private void printResult(MockFlowFile result, TestSetupRead setup, long start, long finish){
        long countWrite;

        countWrite = Long.parseLong(result.getAttribute(CQLAttributes.READ_COUNT));
        System.out.printf("Source: '%s'; READ; '%s': %s (%d ms); Items: %d; Perf: %.1f [calls/sec]%s",
                setup.name,
                "FlowFile",
                ReadableValue.fromMillisecond(finish - start),
                finish - start,
                countWrite,
                countWrite / ((finish - start) / 1000.0),
                System.lineSeparator());
    }

    private List<MockFlowFile> coreTestParallel(TestSetupRead setup, String content, boolean validate, int parallel) throws Exception {
        try {
            long finish, start, countWrite;
            List<MockFlowFile> results;
            boolean ok;

            start = System.currentTimeMillis();
            testRunner.run(parallel);
            if (!testRunner.getFlowFilesForRelationship(PutCQL.REL_SUCCESS).isEmpty()) {
                results = testRunner.getFlowFilesForRelationship(PutCQL.REL_SUCCESS);
                ok = testRunner.getFlowFilesForRelationship(PutCQL.REL_FAILURE).isEmpty();
                finish = System.currentTimeMillis();

                if (ok) {
                    for (MockFlowFile result: results)
                        printResult(result, setup, start, finish);
                    return results;
                }
            }
        }
        catch (Exception ex) {
            throw new Exception("Error in PROCESSOR");
        }
        return null;
    }

    private MockFlowFile coreTest(TestSetupRead setup, String content, boolean validate) throws Exception {
        try {
            long finish, start, countWrite;
            MockFlowFile result;
            boolean ok;

            start = System.currentTimeMillis();
            testRunner.run();
            if (!testRunner.getFlowFilesForRelationship(PutCQL.REL_SUCCESS).isEmpty()) {
                result = testRunner.getFlowFilesForRelationship(PutCQL.REL_SUCCESS).getLast();
                ok = testRunner.getFlowFilesForRelationship(PutCQL.REL_FAILURE).isEmpty();
                finish = System.currentTimeMillis();

                if (ok) {
                    printResult(result, setup, start, finish);
                    return result;
                }
            }
        }
        catch (Exception ex) {
            throw new Exception("Error in PROCESSOR");
        }
        return null;
    }

}
