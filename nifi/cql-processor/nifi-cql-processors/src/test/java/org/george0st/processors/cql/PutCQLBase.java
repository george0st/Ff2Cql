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

    protected FlowFile coreTest(TestSetup setup, String content) {
        return coreTest(setup, content, null, null);
    }

    protected FlowFile coreTest(TestSetup setup, String content, PropertyDescriptor property, String propertyValue){
        HashMap<String, String> attributes = new HashMap<String, String>(Map.of("CQLName",setup.name));
        FlowFile result;

        testRunner.enqueue(content, attributes);
        setup.setProperty(testRunner, testService);
        if (property != null)
            setup.setProperty(testRunner, property, propertyValue);
        testRunner.enableControllerService(testService);
        result = coreTest();
        testRunner.disableControllerService(testService);
        return result;
    }

    private FlowFile coreTest(){
        try {
            long finish, start, count;
            FlowFile result;
            boolean ok;

            start = System.currentTimeMillis();
            testRunner.run();
            result = testRunner.getFlowFilesForRelationship(PutCQL.REL_SUCCESS).getLast();
            ok = testRunner.getFlowFilesForRelationship(PutCQL.REL_FAILURE).isEmpty();
            finish = System.currentTimeMillis();

            if (ok) {
                count = Long.parseLong(result.getAttribute(PutCQL.ATTRIBUTE_COUNT));
                System.out.printf("SetupName: '%s'; '%s': %s (%d ms); Items: %d; Perf: %.1f [calls/sec]%s",
                        result.getAttribute("CQLName"),
                        "FlowFile",
                        ReadableValue.fromMillisecond(finish - start),
                        finish - start,
                        count,
                        count / ((finish - start) / 1000.0),
                        System.lineSeparator());
                return result;
            }
            return null;
        }
        catch(Exception ex) {
            return null;
        }
    }

}
