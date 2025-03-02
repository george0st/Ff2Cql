package org.george0st.processors.cql;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.george0st.cql.CQLControllerService;
import org.george0st.processors.cql.helper.CqlCreateSchema;
import org.george0st.processors.cql.helper.TestSetup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
                CqlCreateSchema schema = new CqlCreateSchema(session, setup);
                schema.Create();
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

}
