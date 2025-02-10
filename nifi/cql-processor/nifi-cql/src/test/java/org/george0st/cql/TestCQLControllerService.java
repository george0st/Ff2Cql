/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.george0st.cql;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.george0st.cql.helper.TestControllerSetup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Marker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// https://medium.com/hashmapinc/creating-custom-processors-and-controllers-in-apache-nifi-e14148740ea
public class TestCQLControllerService {

    private List<TestControllerSetup> setups;


    @BeforeEach
    public void init() {

    }

    // region internal
    private void addTestScope(TestRunner testRunner, CQLControllerService service, String propertyFile) throws IOException {
        TestControllerSetup itm;

        itm = TestControllerSetup.getInstance(testRunner, service, propertyFile);
        if (itm!=null) {
            setups.add(itm);
            testRunner.getLogger().info("Test scope: '{}'", itm.name);
        }
    }

    private void initTestScope(TestRunner testRunner, CQLControllerService service) throws IOException {
        if (setups == null) {
            setups = new ArrayList<TestControllerSetup>();

            addTestScope(testRunner, service,
                    TestControllerSetup.getTestPropertyFile(new String[]{"test-cassandra-private.json", "test-properties.json"}));
            addTestScope(testRunner, service,
                    TestControllerSetup.getTestPropertyFile(new String[]{"test-scylla-private.json", "test-properties.json"}));
        }
    }

    // endregion

    //  region Setting - Valid
    @Test
    public void testSettingValid() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final CQLControllerService service = new CQLControllerService();

        runner.addControllerService("test-good", service);
        runner.setValidateExpressionUsage(false);
        initTestScope(runner, service);

        for (TestControllerSetup controllerSetup: setups) {
            controllerSetup.setProperty();
            runner.assertValid(service);
        }
    }
    // endregion

    // region Setting - NotValid
    @Test
    public void testSettingNotValidIPAddresses() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final CQLControllerService service = new CQLControllerService();

        runner.addControllerService("test-good", service);
        runner.setValidateExpressionUsage(false);
        initTestScope(runner, service);

        for (TestControllerSetup controllerSetup: setups) {
            controllerSetup.setProperty();
            runner.setProperty(service, CQLControllerService.IP_ADDRESSES, "");     //  err
            runner.assertNotValid(service);
        }
    }

    @Test
    public void testSettingNotValidPort() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final CQLControllerService service = new CQLControllerService();

        runner.addControllerService("test-good", service);
        runner.setValidateExpressionUsage(false);
        initTestScope(runner, service);

        for (TestControllerSetup controllerSetup: setups) {
            controllerSetup.setProperty();
            runner.setProperty(service, CQLControllerService.PORT, "aa");           //  err
            runner.assertNotValid(service);
        }
    }

    @Test
    public void testSettingNotValidConnectionTimeout() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final CQLControllerService service = new CQLControllerService();

        runner.addControllerService("test-good", service);
        runner.setValidateExpressionUsage(false);
        initTestScope(runner, service);

        for (TestControllerSetup controllerSetup: setups) {
            controllerSetup.setProperty();
            runner.setProperty(service, CQLControllerService.CONNECTION_TIMEOUT, "ee");     //  err
            runner.assertNotValid(service);
        }
    }

    @Test
    public void testSettingNotValidRequestTimeout() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final CQLControllerService service = new CQLControllerService();

        runner.addControllerService("test-good", service);
        runner.setValidateExpressionUsage(false);
        initTestScope(runner, service);

        for (TestControllerSetup controllerSetup: setups) {
            controllerSetup.setProperty();
            runner.setProperty(service, CQLControllerService.REQUEST_TIMEOUT, "qq");        //  err
            runner.assertNotValid(service);
        }
    }

    @Test
    public void testSettingNotValidConsistencyLevel() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final CQLControllerService service = new CQLControllerService();

        runner.addControllerService("test-good", service);
        runner.setValidateExpressionUsage(false);
        initTestScope(runner, service);

        for (TestControllerSetup controllerSetup: setups) {
            controllerSetup.setProperty();
            runner.setProperty(service, CQLControllerService.CONSISTENCY_LEVEL, "LOCAL");   //  err
            runner.assertNotValid(service);
        }
    }

    // endregion

    // region Connection
    @Test
    public void testConnection() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final CQLControllerService service = new CQLControllerService();

        runner.addControllerService("test-good", service);
        runner.setValidateExpressionUsage(false);
        initTestScope(runner, service);

        for (TestControllerSetup controllerSetup: setups) {
            controllerSetup.setProperty();
            runner.enableControllerService(service);
            runner.disableControllerService(service);
        }
    }

    @Test
    public void testConnectionAndSession() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final CQLControllerService service = new CQLControllerService();

        runner.addControllerService("test-good", service);
        runner.setValidateExpressionUsage(false);
        initTestScope(runner, service);

        for (TestControllerSetup controllerSetup: setups) {
            controllerSetup.setProperty();
            runner.enableControllerService(service);

            //  test session
            try (CqlSession session = service.getSession()){
            }
            runner.disableControllerService(service);
        }
    }

    // endregion

    // region Others

    @Test
    public void testURI() throws InitializationException, IOException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final CQLControllerService service = new CQLControllerService();

        runner.addControllerService("test-good", service);
        runner.setValidateExpressionUsage(false);
        initTestScope(runner, service);

        for (TestControllerSetup controllerSetup: setups) {
            controllerSetup.setProperty();
            runner.enableControllerService(service);
            runner.getLogger().info("getURI: '{}'", service.getURI());
            runner.disableControllerService(service);
        }
    }

    // endregion
}
