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

    public GetCQLBase() throws Exception {
        //  create TestSetupWrite scope based on test-*.json files
        setups = createSetup();

        //  Prepare data for reading (including build schema, if not exist)
        PutCQLFunction putCQL=new PutCQLFunction();
        putCQL.init();
        putCQL.testBasic();
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

    protected MockFlowFile runTest(TestSetupRead setup, String content) throws Exception {
        return runTestWithProperty(setup, content, null, null, false);
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
                    countWrite = Long.parseLong(result.getAttribute(CQLAttributes.READ_COUNT));
                    System.out.printf("Source: '%s'; WRITE; '%s': %s (%d ms); Items: %d; Perf: %.1f [calls/sec]%s",
                            setup.name,
                            "FlowFile",
                            ReadableValue.fromMillisecond(finish - start),
                            finish - start,
                            countWrite,
                            countWrite / ((finish - start) / 1000.0),
                            System.lineSeparator());

//                    if (validate) {
//                        // delay (before read for synch on CQL side)
//                        Thread.sleep(3000);
//
//                        // validate (read value from CSV and from CQL and compare content)
//                        try (CqlSession session = testService.getSession()) {
//                            start = System.currentTimeMillis();
//                            countRead = (new CsvCqlValidate(session, setup, CqlTestSchema.primaryKeys)).executeContent(content);
//                            finish = System.currentTimeMillis();
//                        }
//                        System.out.printf("Source: '%s'; VALIDATE; '%s': %s (%d ms); Items: %d; Perf: %.1f [calls/sec]%s",
//                                result.getAttribute("CQLName"),
//                                "FlowFile",
//                                ReadableValue.fromMillisecond(finish - start),
//                                finish - start,
//                                countRead,
//                                countRead / ((finish - start) / 1000.0),
//                                System.lineSeparator());
//
//                        if (countWrite != countRead)
//                            throw new Exception("The amount of Write and Read operations are different");
//                    }
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
