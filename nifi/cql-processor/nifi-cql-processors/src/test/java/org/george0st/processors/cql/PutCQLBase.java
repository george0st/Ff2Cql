package org.george0st.processors.cql;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.george0st.cql.CQLControllerService;
import org.george0st.processors.cql.helper.CqlTestSchema;
import org.george0st.processors.cql.helper.ReadableValue;
import org.george0st.processors.cql.helper.TestSetup;
import org.george0st.processors.cql.processor.CsvCqlValidate;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PutCQLBase {

    protected TestRunner testRunner;
    protected CQLControllerService testService;
    protected List<TestSetup> setups;

    public PutCQLBase() throws IOException, InitializationException, InterruptedException {
        //  create TestSetup scope based on test-*.json files
        setups = createSetup();

        //  prepare test scope
        TestRunner runner = TestRunners.newTestRunner(PutCQL.class);
        CQLControllerService service = new CQLControllerService();
        runner.addControllerService(PutCQL.SERVICE_CONTROLLER.getName(), service);
        for (TestSetup setup: setups) {
            setup.setProperty(runner, service);
            runner.enableControllerService(service);

            //  build schema, if needed
            try (CqlSession session=service.getSession()) {
                CqlTestSchema schema = new CqlTestSchema(session, setup);
                if (!schema.createSchema())
                    schema.cleanData();
            }
            runner.disableControllerService(service);
        }
    }

    protected ArrayList<TestSetup> createSetup() throws IOException {
        ArrayList<TestSetup> setup= new ArrayList<TestSetup>();

        addTestScope(setup,
                TestSetup.getTestPropertyFile("./src/test",
                        new String[]{"test-cassandra.json", "test-properties.json"}));
        addTestScope(setup,
                TestSetup.getTestPropertyFile("./src/test",
                        new String[]{"test-scylla.json", "test-properties.json"}));
        addTestScope(setup,
                TestSetup.getTestPropertyFile("./src/test",
                        new String[]{"test-astra.json", "test-properties.json"}));
        return setup;
    }

    protected void addTestScope(List<TestSetup> setup, String propertyFile) throws IOException {
        TestSetup itm;

        itm = TestSetup.getInstance(propertyFile);
        if (itm!=null) setup.add(itm);
    }

    @BeforeEach
    public void init() throws IOException, InterruptedException, InitializationException {
        testRunner = TestRunners.newTestRunner(PutCQL.class);
        testService = new CQLControllerService();
        testRunner.addControllerService(PutCQL.SERVICE_CONTROLLER.getName(), testService);

        for (TestSetup setup: setups) {
            System.out.println(String.format("Test scope: '%s'", setup.name));
        }
    }

    @AfterEach
    public void Close() throws InterruptedException {
        for (TestSetup setup: setups) {
            setup.setProperty(testRunner, testService);
            testRunner.enableControllerService(testService);

            //  build schema, if needed
            try (CqlSession session=testService.getSession()) {
                CqlTestSchema schema = new CqlTestSchema(session, setup);
                schema.cleanData();
            }
            testRunner.disableControllerService(testService);
        }
    }

    protected FlowFile runTest(TestSetup setup, String content) throws Exception {
        return runTestWithProperty(setup, content, null, null, false);
    }

    protected FlowFile runTest(TestSetup setup, String content, boolean validate) throws Exception {
        return runTestWithProperty(setup, content, null, null, validate);
    }

    protected FlowFile runTestWithProperty(TestSetup setup, String content, PropertyDescriptor property, String propertyValue) throws Exception {
        return  runTestWithProperty(setup, content, property, propertyValue, false);
    }

    protected FlowFile runTestWithProperty(TestSetup setup, String content, PropertyDescriptor property, String propertyValue, boolean validate) throws Exception {
        HashMap<String, String> attributes = new HashMap<String, String>(Map.of("CQLName",setup.name));
        FlowFile result;

        testRunner.enqueue(content, attributes);
        setup.setProperty(testRunner, testService);
        if (property != null)
            setup.setProperty(testRunner, property, propertyValue);
        testRunner.enableControllerService(testService);
        result = coreTest(setup, content, validate);
        testRunner.disableControllerService(testService);
        return result;
    }

    private FlowFile coreTest(TestSetup setup, String content, boolean validate) throws Exception {
        try {
            long finish, start, countWrite, countRead;
            FlowFile result;
            boolean ok;

            start = System.currentTimeMillis();
            testRunner.run();
            if (!testRunner.getFlowFilesForRelationship(PutCQL.REL_SUCCESS).isEmpty()) {
                result = testRunner.getFlowFilesForRelationship(PutCQL.REL_SUCCESS).getLast();
                ok = testRunner.getFlowFilesForRelationship(PutCQL.REL_FAILURE).isEmpty();
                finish = System.currentTimeMillis();

                if (ok) {
                    countWrite = Long.parseLong(result.getAttribute(PutCQL.ATTRIBUTE_COUNT));
                    System.out.printf("Source: '%s'; WRITE; '%s': %s (%d ms); Items: %d; Perf: %.1f [calls/sec]%s",
                            result.getAttribute("CQLName"),
                            "FlowFile",
                            ReadableValue.fromMillisecond(finish - start),
                            finish - start,
                            countWrite,
                            countWrite / ((finish - start) / 1000.0),
                            System.lineSeparator());

                    if (validate) {
                        // delay (before read for synch on CQL side)
                        Thread.sleep(3000);

                        // validate (read value from CSV and from CQL and compare content)
                        try (CqlSession session = testService.getSession()) {
                            start = System.currentTimeMillis();
                            countRead = (new CsvCqlValidate(session, setup, CqlTestSchema.primaryKeys)).executeContent(content);
                            finish = System.currentTimeMillis();
                        }
                        System.out.printf("Source: '%s'; VALIDATE; '%s': %s (%d ms); Items: %d; Perf: %.1f [calls/sec]%s",
                                result.getAttribute("CQLName"),
                                "FlowFile",
                                ReadableValue.fromMillisecond(finish - start),
                                finish - start,
                                countRead,
                                countRead / ((finish - start) / 1000.0),
                                System.lineSeparator());

                        if (countWrite != countRead)
                            throw new Exception("The amount of Write and Read operations are different");
                    }
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
