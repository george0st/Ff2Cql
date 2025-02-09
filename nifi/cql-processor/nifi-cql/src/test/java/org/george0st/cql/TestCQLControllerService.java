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

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

// https://medium.com/hashmapinc/creating-custom-processors-and-controllers-in-apache-nifi-e14148740ea
public class TestCQLControllerService {

    @BeforeEach
    public void init() {

    }

    @Test
    public void testSettingValid() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final CQLControllerService service = new CQLControllerService();

        runner.addControllerService("test-good", service);

        runner.setProperty(service, CQLControllerService.IP_ADDRESSES, "10.129.53.159 , 10.129.53.154  , 10.129.53.153");
        runner.setProperty(service, CQLControllerService.PORT, "9042");
        runner.setProperty(service, CQLControllerService.USERNAME, "perf");
        runner.setProperty(service, CQLControllerService.PASSWORD, "perf");
        runner.setProperty(service, CQLControllerService.LOCAL_DC, "datacenter1");
        runner.setProperty(service, CQLControllerService.CONNECTION_TIMEOUT, "90");
        runner.setProperty(service, CQLControllerService.REQUEST_TIMEOUT, "90");
        runner.setProperty(service, CQLControllerService.CONSISTENCY_LEVEL, "LOCAL_ONE");
        runner.setValidateExpressionUsage(false);
        //runner.enableControllerService(service);

        runner.assertValid(service);
    }

    @Test
    public void testSettingNotValidPort() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final CQLControllerService service = new CQLControllerService();

        runner.addControllerService("test-good", service);

        runner.setProperty(service, CQLControllerService.IP_ADDRESSES, "10.129.53.159 , 10.129.53.154  , 10.129.53.153");
        runner.setProperty(service, CQLControllerService.PORT, "aa");
        runner.setProperty(service, CQLControllerService.USERNAME, "perf");
        runner.setProperty(service, CQLControllerService.PASSWORD, "perf");
        runner.setProperty(service, CQLControllerService.LOCAL_DC, "datacenter1");
        runner.setProperty(service, CQLControllerService.CONNECTION_TIMEOUT, "90");
        runner.setProperty(service, CQLControllerService.REQUEST_TIMEOUT, "90");
        runner.setProperty(service, CQLControllerService.CONSISTENCY_LEVEL, "LOCAL_ONE");
        runner.setValidateExpressionUsage(false);
        //runner.enableControllerService(service);

        //  invalid port
        runner.assertNotValid(service);
    }

    @Test
    public void testSettingNotValidConsistencyLevel() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final CQLControllerService service = new CQLControllerService();

        runner.addControllerService("test-good", service);

        runner.setProperty(service, CQLControllerService.IP_ADDRESSES, "10.129.53.159 , 10.129.53.154  , 10.129.53.153");
        runner.setProperty(service, CQLControllerService.PORT, "9042");
        runner.setProperty(service, CQLControllerService.USERNAME, "perf");
        runner.setProperty(service, CQLControllerService.PASSWORD, "perf");
        runner.setProperty(service, CQLControllerService.LOCAL_DC, "datacenter1");
        runner.setProperty(service, CQLControllerService.CONNECTION_TIMEOUT, "90");
        runner.setProperty(service, CQLControllerService.REQUEST_TIMEOUT, "90");
        runner.setProperty(service, CQLControllerService.CONSISTENCY_LEVEL, "LOCAL");
        runner.setValidateExpressionUsage(false);
        //runner.enableControllerService(service);

        //  invalid port
        runner.assertNotValid(service);
    }

    @Test
    public void testConnection() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final CQLControllerService service = new CQLControllerService();

        runner.addControllerService("test-good", service);

        runner.setProperty(service, CQLControllerService.IP_ADDRESSES, "10.129.53.159 , 10.129.53.154  , 10.129.53.153");
        runner.setProperty(service, CQLControllerService.PORT, "9042");
        runner.setProperty(service, CQLControllerService.USERNAME, "perf");
        runner.setProperty(service, CQLControllerService.PASSWORD, "perf");
        runner.setProperty(service, CQLControllerService.LOCAL_DC, "datacenter1");
        runner.setProperty(service, CQLControllerService.CONNECTION_TIMEOUT, "90");
        runner.setProperty(service, CQLControllerService.REQUEST_TIMEOUT, "90");
        runner.setProperty(service, CQLControllerService.CONSISTENCY_LEVEL, "LOCAL_ONE");
        runner.setValidateExpressionUsage(false);
        runner.enableControllerService(service);
    }

}
