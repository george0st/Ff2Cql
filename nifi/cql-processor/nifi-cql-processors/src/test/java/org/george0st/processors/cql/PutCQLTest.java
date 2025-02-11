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
package org.george0st.processors.cql;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.george0st.cql.CQLControllerService;
import org.george0st.processors.cql.helper.CqlCreateSchema;
import org.george0st.processors.cql.helper.ReadableValue;
import org.george0st.processors.cql.helper.TestSetup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import java.io.IOException;
import java.util.*;

public class PutCQLTest {

    private TestRunner testRunner;
    private CQLControllerService testService;
    private List<TestSetup> setups;

    // Helper
    // https://medium.com/@mr.sinchan.banerjee/nifi-custom-processor-series-part-3-junit-test-with-nifi-mock-a935a1a4e3e5


//    public TestRunner init2(Class processor) throws Exception {
//        TestRunner runner = TestRunners.newTestRunner(processor);
//        runner.addControllerService("clientService", clientService);
//        runner.setProperty(clientService, MongoDBControllerService.URI, MONGO_CONTAINER.getConnectionString());
//        runner.setProperty(AbstractMongoProcessor.CLIENT_SERVICE, "clientService");
//        runner.enableControllerService(clientService);
//        runner.setEnvironmentVariableValue("uri", MONGO_CONTAINER.getConnectionString());
//        runner.setEnvironmentVariableValue("db", DATABASE_NAME);
//        runner.setEnvironmentVariableValue("collection", COLLECTION_NAME);
//        runner.setProperty(AbstractMongoProcessor.DATABASE_NAME, "${db}");
//        runner.setProperty(AbstractMongoProcessor.COLLECTION_NAME, "${collection}");
//        return runner;
//    }

    private void addTestScope(TestRunner testRunner, CQLControllerService testService, String propertyFile) throws IOException {
        TestSetup itm;

        itm = TestSetup.getInstance(testRunner, testService, propertyFile);
        if (itm!=null) {
            setups.add(itm);
            testRunner.getLogger().info("Test scope: '{}'", itm.name);
        }
    }

    @BeforeEach
    public void init() throws IOException, InterruptedException, InitializationException {

        testRunner = TestRunners.newTestRunner(PutCQL.class);
        testService = new CQLControllerService();
        testRunner.addControllerService(PutCQL.SERVICE_CONTROLLER.getName(), testService);

        if (setups == null) {
            setups = new ArrayList<TestSetup>();

            addTestScope(testRunner, testService,
                    TestSetup.getTestPropertyFile(new String[]{"test-cassandra-private.json", "test-properties.json"}));
            addTestScope(testRunner, testService,
                    TestSetup.getTestPropertyFile(new String[]{"test-scylla-private.json", "test-properties.json"}));
        }

//        //  build schema
//        for (TestSetup setup: setups) {
//            CqlSession session=null;
//            CqlCreateSchema schema = new CqlCreateSchema(session, setup);
//            schema.Create();
//        }

    }

    private FlowFile coreTest(){
        try{
            long finish, start, count;
            FlowFile result;

            start = System.currentTimeMillis();
            testRunner.run();
            result = testRunner.getFlowFilesForRelationship(PutCQL.REL_SUCCESS).getLast();
            finish = System.currentTimeMillis();

            count=Long.parseLong(result.getAttribute(PutCQL.ATTRIBUTE_COUNT));
            System.out.printf("SetupName: '%s'; '%s': %s (%d ms); Items: %d; Perf: %.1f [calls/sec]%s",
                    result.getAttribute("CQLName"),
                    "FlowFile",
                    ReadableValue.fromMillisecond(finish - start),
                    finish-start,
                    count,
                    count / ((finish - start) / 1000.0),
                    System.lineSeparator());
            return result;
        }
        catch(Exception ex) {
            return null;
        }
    }

    @Test
    public void testBasic() {

        HashMap<String, String> attributes = new HashMap<String, String>();

        String content = "\"colbigint\",\"colint\",\"coltext\",\"colfloat\",\"coldouble\",\"coldate\",\"coltime\",\"coltimestamp\",\"colboolean\",\"coluuid\",\"colsmallint\",\"coltinyint\",\"coltimeuuid\",\"colvarchar\"\n" +
                "\"0\",\"1064\",\"zeVOKGnORq\",\"627.6811\",\"395.8522407512559\",\"1971-11-12\",\"03:37:15\",\"2000-09-25T22:18:45Z\",\"false\",\"6080071f-4dd1-4ea5-b711-9ad0716e242a\",\"8966\",\"55\",\"f45e58f5-c3b7-11ef-8d19-97ae87be7c54\",\"Tzxsw\"\n" +
                "\"1\",\"1709\",\"7By0z5QEXh\",\"652.03955\",\"326.9081263857284\",\"2013-12-17\",\"08:43:09\",\"2010-04-27T07:02:27Z\",\"false\",\"7d511666-2f81-41c4-9d5c-a5fa87f7d1c3\",\"24399\",\"38\",\"f45e8006-c3b7-11ef-8d19-172ff8d0d752\",\"exAbN\"\n" +
                "\"2\",\"6249\",\"UYI6AgkcBt\",\"939.01556\",\"373.48559413289485\",\"1980-11-05\",\"15:44:43\",\"2023-11-24T05:59:12Z\",\"false\",\"dbd35d1b-38d0-49a4-8069-9efd68314dc5\",\"6918\",\"72\",\"f45e8007-c3b7-11ef-8d19-d784fa8af8e3\",\"IjnDb\"\n" +
                "\"3\",\"6998\",\"lXQ69C5HOZ\",\"715.1224\",\"236.7994939033784\",\"1992-02-01\",\"08:07:34\",\"2024-06-29T21:08:54.463Z\",\"true\",\"84a7395c-94fd-43f5-84c6-4152f0407e93\",\"22123\",\"39\",\"f45e8008-c3b7-11ef-8d19-0376318d55df\",\"jyZo8\"\n";
        FlowFile result;

        for (TestSetup setup : setups) {
            attributes.put("CQLName", setup.name);

            testRunner.enqueue(content, attributes);
            setup.setProperty();
            testRunner.enableControllerService(testService);
            result = coreTest();
            testRunner.disableControllerService(testService);

            //  check result
            assertNotNull(result, String.format("Issue with processing in '%s'", setup.name));
            //  check amount of write items
            assertEquals(Long.parseLong(result.getAttribute(PutCQL.ATTRIBUTE_COUNT)),4);
        }
    }

    @Test
    public void Test2(){

    }
//
//    @Test
//    public void testBasic2ItemsSameSetup() {
//
//        HashMap<String, String> attributes = new HashMap<String, String>() {{
//            put("xxxx", "yyyy");
//            put("aa", "bb");
//        }};
//
//        String content = "\"colbigint\",\"colint\",\"coltext\",\"colfloat\",\"coldouble\",\"coldate\",\"coltime\",\"coltimestamp\",\"colboolean\",\"coluuid\",\"colsmallint\",\"coltinyint\",\"coltimeuuid\",\"colvarchar\"\n" +
//                "\"0\",\"1064\",\"zeVOKGnORq\",\"627.6811\",\"395.8522407512559\",\"1971-11-12\",\"03:37:15\",\"2000-09-25T22:18:45Z\",\"false\",\"6080071f-4dd1-4ea5-b711-9ad0716e242a\",\"8966\",\"55\",\"f45e58f5-c3b7-11ef-8d19-97ae87be7c54\",\"Tzxsw\"\n" +
//                "\"1\",\"1709\",\"7By0z5QEXh\",\"652.03955\",\"326.9081263857284\",\"2013-12-17\",\"08:43:09\",\"2010-04-27T07:02:27Z\",\"false\",\"7d511666-2f81-41c4-9d5c-a5fa87f7d1c3\",\"24399\",\"38\",\"f45e8006-c3b7-11ef-8d19-172ff8d0d752\",\"exAbN\"\n" +
//                "\"2\",\"6249\",\"UYI6AgkcBt\",\"939.01556\",\"373.48559413289485\",\"1980-11-05\",\"15:44:43\",\"2023-11-24T05:59:12Z\",\"false\",\"dbd35d1b-38d0-49a4-8069-9efd68314dc5\",\"6918\",\"72\",\"f45e8007-c3b7-11ef-8d19-d784fa8af8e3\",\"IjnDb\"\n" +
//                "\"3\",\"6998\",\"lXQ69C5HOZ\",\"715.1224\",\"236.7994939033784\",\"1992-02-01\",\"08:07:34\",\"1998-04-09T23:19:18Z\",\"true\",\"84a7395c-94fd-43f5-84c6-4152f0407e93\",\"22123\",\"39\",\"f45e8008-c3b7-11ef-8d19-0376318d55df\",\"jyZo8\"\n";
//
//        String content2 = "\"colbigint\",\"colint\",\"coltext\",\"colfloat\",\"coldouble\",\"coldate\",\"coltime\",\"coltimestamp\",\"colboolean\",\"coluuid\",\"colsmallint\",\"coltinyint\",\"coltimeuuid\",\"colvarchar\"\n" +
//                "\"10\",\"1064\",\"zeVOKGnORq\",\"627.6811\",\"395.8522407512559\",\"1971-11-12\",\"03:37:15\",\"2000-09-25T22:18:45Z\",\"false\",\"6080071f-4dd1-4ea5-b711-9ad0716e242a\",\"8966\",\"55\",\"f45e58f5-c3b7-11ef-8d19-97ae87be7c54\",\"Tzxsw\"\n" +
//                "\"11\",\"1709\",\"7By0z5QEXh\",\"652.03955\",\"326.9081263857284\",\"2013-12-17\",\"08:43:09\",\"2010-04-27T07:02:27Z\",\"false\",\"7d511666-2f81-41c4-9d5c-a5fa87f7d1c3\",\"24399\",\"38\",\"f45e8006-c3b7-11ef-8d19-172ff8d0d752\",\"exAbN\"\n" +
//                "\"12\",\"6249\",\"UYI6AgkcBt\",\"939.01556\",\"373.48559413289485\",\"1980-11-05\",\"15:44:43\",\"2023-11-24T05:59:12Z\",\"false\",\"dbd35d1b-38d0-49a4-8069-9efd68314dc5\",\"6918\",\"72\",\"f45e8007-c3b7-11ef-8d19-d784fa8af8e3\",\"IjnDb\"\n" +
//                "\"13\",\"6998\",\"lXQ69C5HOZ\",\"715.1224\",\"236.7994939033784\",\"1992-02-01\",\"08:07:34\",\"1998-04-09T23:19:18Z\",\"true\",\"84a7395c-94fd-43f5-84c6-4152f0407e93\",\"22123\",\"39\",\"f45e8008-c3b7-11ef-8d19-0376318d55df\",\"jyZo8\"\n";
//
//        FlowFile result;
//
//        for (TestSetup controllerSetup: setups) {
//            attributes.put("CQLName",controllerSetup.name);
//
//            testRunner.enqueue(content, attributes);
//            controllerSetup.setProperty();
//            controllerSetup.setProperty(PutCQL.BATCH_SIZE, "350");
//            controllerSetup.setProperty(PutCQL.DRY_RUN, "false");
//            result = coreTest();
//            assertEquals(ControllerSetup.CompareStatus.CHANGE_ACCESS.name(), result.getAttribute(PutCQL.ATTRIBUTE_COMPARE_STATUS));
//
//            testRunner.enqueue(content2, attributes);
//            controllerSetup.setProperty();
//            controllerSetup.setProperty(PutCQL.BATCH_SIZE, "350");
//            controllerSetup.setProperty(PutCQL.DRY_RUN, "false");
//            result = coreTest();
//            assertEquals(ControllerSetup.CompareStatus.SAME.name(), result.getAttribute(PutCQL.ATTRIBUTE_COMPARE_STATUS));
//        }
//    }
//
//    @Test
//    public void testBasic3ItemsDifferentSetup() {
//
//        HashMap<String, String> attributes = new HashMap<String, String>() {{
//            put("xxxx", "yyyy");
//            put("aa", "bb");
//        }};
//
//        String content = "\"colbigint\",\"colint\",\"coltext\",\"colfloat\",\"coldouble\",\"coldate\",\"coltime\",\"coltimestamp\",\"colboolean\",\"coluuid\",\"colsmallint\",\"coltinyint\",\"coltimeuuid\",\"colvarchar\"\n" +
//                "\"0\",\"1064\",\"zeVOKGnORq\",\"627.6811\",\"395.8522407512559\",\"1971-11-12\",\"03:37:15\",\"2000-09-25T22:18:45Z\",\"false\",\"6080071f-4dd1-4ea5-b711-9ad0716e242a\",\"8966\",\"55\",\"f45e58f5-c3b7-11ef-8d19-97ae87be7c54\",\"Tzxsw\"\n" +
//                "\"1\",\"1709\",\"7By0z5QEXh\",\"652.03955\",\"326.9081263857284\",\"2013-12-17\",\"08:43:09\",\"2010-04-27T07:02:27Z\",\"false\",\"7d511666-2f81-41c4-9d5c-a5fa87f7d1c3\",\"24399\",\"38\",\"f45e8006-c3b7-11ef-8d19-172ff8d0d752\",\"exAbN\"\n" +
//                "\"2\",\"6249\",\"UYI6AgkcBt\",\"939.01556\",\"373.48559413289485\",\"1980-11-05\",\"15:44:43\",\"2023-11-24T05:59:12Z\",\"false\",\"dbd35d1b-38d0-49a4-8069-9efd68314dc5\",\"6918\",\"72\",\"f45e8007-c3b7-11ef-8d19-d784fa8af8e3\",\"IjnDb\"\n" +
//                "\"3\",\"6998\",\"lXQ69C5HOZ\",\"715.1224\",\"236.7994939033784\",\"1992-02-01\",\"08:07:34\",\"1998-04-09T23:19:18Z\",\"true\",\"84a7395c-94fd-43f5-84c6-4152f0407e93\",\"22123\",\"39\",\"f45e8008-c3b7-11ef-8d19-0376318d55df\",\"jyZo8\"\n";
//
//        String content2 = "\"colbigint\",\"colint\",\"coltext\",\"colfloat\",\"coldouble\",\"coldate\",\"coltime\",\"coltimestamp\",\"colboolean\",\"coluuid\",\"colsmallint\",\"coltinyint\",\"coltimeuuid\",\"colvarchar\"\n" +
//                "\"10\",\"1064\",\"zeVOKGnORq\",\"627.6811\",\"395.8522407512559\",\"1971-11-12\",\"03:37:15\",\"2000-09-25T22:18:45Z\",\"false\",\"6080071f-4dd1-4ea5-b711-9ad0716e242a\",\"8966\",\"55\",\"f45e58f5-c3b7-11ef-8d19-97ae87be7c54\",\"Tzxsw\"\n" +
//                "\"11\",\"1709\",\"7By0z5QEXh\",\"652.03955\",\"326.9081263857284\",\"2013-12-17\",\"08:43:09\",\"2010-04-27T07:02:27Z\",\"false\",\"7d511666-2f81-41c4-9d5c-a5fa87f7d1c3\",\"24399\",\"38\",\"f45e8006-c3b7-11ef-8d19-172ff8d0d752\",\"exAbN\"\n" +
//                "\"12\",\"6249\",\"UYI6AgkcBt\",\"939.01556\",\"373.48559413289485\",\"1980-11-05\",\"15:44:43\",\"2023-11-24T05:59:12Z\",\"false\",\"dbd35d1b-38d0-49a4-8069-9efd68314dc5\",\"6918\",\"72\",\"f45e8007-c3b7-11ef-8d19-d784fa8af8e3\",\"IjnDb\"\n" +
//                "\"13\",\"6998\",\"lXQ69C5HOZ\",\"715.1224\",\"236.7994939033784\",\"1992-02-01\",\"08:07:34\",\"1998-04-09T23:19:18Z\",\"true\",\"84a7395c-94fd-43f5-84c6-4152f0407e93\",\"22123\",\"39\",\"f45e8008-c3b7-11ef-8d19-0376318d55df\",\"jyZo8\"\n";
//        FlowFile result;
//
//        for (TestSetup controllerSetup: setups) {
//            attributes.put("CQLName",controllerSetup.name);
//
//            testRunner.enqueue(content, attributes);
//            controllerSetup.setProperty();
//            controllerSetup.setProperty(PutCQL.BATCH_SIZE, "350");
//            controllerSetup.setProperty(PutCQL.DRY_RUN, "false");
//            result = coreTest();
//            assertEquals(ControllerSetup.CompareStatus.CHANGE_ACCESS.name(), result.getAttribute(PutCQL.ATTRIBUTE_COMPARE_STATUS));
//
//            testRunner.enqueue(content, attributes);
//            controllerSetup.setProperty();
//            controllerSetup.setProperty(PutCQL.BATCH_SIZE, "350");
//            controllerSetup.setProperty(PutCQL.DRY_RUN, "false");
//            result = coreTest();
//            assertEquals(ControllerSetup.CompareStatus.SAME.name(), result.getAttribute(PutCQL.ATTRIBUTE_COMPARE_STATUS));
//
//            testRunner.enqueue(content2, attributes);
//            controllerSetup.setProperty();
//            controllerSetup.setProperty(PutCQL.BATCH_SIZE, "150");
//            controllerSetup.setProperty(PutCQL.DRY_RUN, "false");
//            result = coreTest();
//            assertEquals(ControllerSetup.CompareStatus.CHANGE.name(), result.getAttribute(PutCQL.ATTRIBUTE_COMPARE_STATUS));
//        }
//    }
//
//    @Test
//    public void testBasic4ItemsSameSetup() {
//
//        HashMap<String, String> attributes = new HashMap<String, String>();
//
//        String content = "\"colbigint\",\"colint\",\"coltext\",\"colfloat\",\"coldouble\",\"coldate\",\"coltime\",\"coltimestamp\",\"colboolean\",\"coluuid\",\"colsmallint\",\"coltinyint\",\"coltimeuuid\",\"colvarchar\"\n" +
//                "\"0\",\"1064\",\"zeVOKGnORq\",\"627.6811\",\"395.8522407512559\",\"1971-11-12\",\"03:37:15\",\"2000-09-25T22:18:45Z\",\"false\",\"6080071f-4dd1-4ea5-b711-9ad0716e242a\",\"8966\",\"55\",\"f45e58f5-c3b7-11ef-8d19-97ae87be7c54\",\"Tzxsw\"\n" +
//                "\"1\",\"1709\",\"7By0z5QEXh\",\"652.03955\",\"326.9081263857284\",\"2013-12-17\",\"08:43:09\",\"2010-04-27T07:02:27Z\",\"false\",\"7d511666-2f81-41c4-9d5c-a5fa87f7d1c3\",\"24399\",\"38\",\"f45e8006-c3b7-11ef-8d19-172ff8d0d752\",\"exAbN\"\n" +
//                "\"2\",\"6249\",\"UYI6AgkcBt\",\"939.01556\",\"373.48559413289485\",\"1980-11-05\",\"15:44:43\",\"2023-11-24T05:59:12Z\",\"false\",\"dbd35d1b-38d0-49a4-8069-9efd68314dc5\",\"6918\",\"72\",\"f45e8007-c3b7-11ef-8d19-d784fa8af8e3\",\"IjnDb\"\n" +
//                "\"3\",\"6998\",\"lXQ69C5HOZ\",\"715.1224\",\"236.7994939033784\",\"1992-02-01\",\"08:07:34\",\"1998-04-09T23:19:18Z\",\"true\",\"84a7395c-94fd-43f5-84c6-4152f0407e93\",\"22123\",\"39\",\"f45e8008-c3b7-11ef-8d19-0376318d55df\",\"jyZo8\"\n";
//
//        FlowFile result;
//
//        for (TestSetup controllerSetup: setups) {
//            attributes.put("CQLName", controllerSetup.name);
//
//            testRunner.enqueue(content, attributes);
//            controllerSetup.setProperty();
//            controllerSetup.setProperty(PutCQL.BATCH_SIZE, "350");
//            controllerSetup.setProperty(PutCQL.DRY_RUN, "false");
//            result = coreTest();
//            assertEquals(ControllerSetup.CompareStatus.CHANGE_ACCESS.name(), result.getAttribute(PutCQL.ATTRIBUTE_COMPARE_STATUS));
//
//            testRunner.enqueue(content, attributes);
//            controllerSetup.setProperty();
//            controllerSetup.setProperty(PutCQL.BATCH_SIZE, "350");
//            controllerSetup.setProperty(PutCQL.DRY_RUN, "false");
//            result = coreTest();
//            assertEquals(ControllerSetup.CompareStatus.SAME.name(), result.getAttribute(PutCQL.ATTRIBUTE_COMPARE_STATUS));
//
//            testRunner.enqueue(content, attributes);
//            controllerSetup.setProperty();
//            controllerSetup.setProperty(PutCQL.BATCH_SIZE, "350");
//            controllerSetup.setProperty(PutCQL.DRY_RUN, "false");
//            result = coreTest();
//            assertEquals(ControllerSetup.CompareStatus.SAME.name(), result.getAttribute(PutCQL.ATTRIBUTE_COMPARE_STATUS));
//
//            testRunner.enqueue(content, attributes);
//            controllerSetup.setProperty();
//            controllerSetup.setProperty(PutCQL.BATCH_SIZE, "350");
//            controllerSetup.setProperty(PutCQL.DRY_RUN, "false");
//            result = coreTest();
//            assertEquals(ControllerSetup.CompareStatus.SAME.name(), result.getAttribute(PutCQL.ATTRIBUTE_COMPARE_STATUS));
//        }
//    }
//
//    @Test
//    public void testEmptyInput() {
//        HashMap<String, String> attributes = new HashMap<String, String>();
//        String content = "";
//        FlowFile result;
//
//        for (TestSetup controllerSetup: setups) {
//            attributes.put("CQLName", controllerSetup.name);
//
//            testRunner.enqueue(content, attributes);
//            controllerSetup.setProperty();
//            controllerSetup.setProperty(PutCQL.BATCH_SIZE, "350");
//            controllerSetup.setProperty(PutCQL.DRY_RUN, "false");
//            result = coreTest();
//            assertEquals(ControllerSetup.CompareStatus.CHANGE_ACCESS.name(), result.getAttribute(PutCQL.ATTRIBUTE_COMPARE_STATUS));
//        }
//    }
//
//    @Test
//    public void testOnlyHeader() {
//        HashMap<String, String> attributes = new HashMap<String, String>();
//        String content = "\"colbigint\",\"colint\",\"coltext\",\"colfloat\",\"coldouble\",\"coldate\",\"coltime\",\"coltimestamp\",\"colboolean\",\"coluuid\",\"colsmallint\",\"coltinyint\",\"coltimeuuid\",\"colvarchar\"\n";
//        FlowFile result;
//
//        for (TestSetup controllerSetup: setups) {
//            attributes.put("CQLName", controllerSetup.name);
//
//            testRunner.enqueue(content, attributes);
//            controllerSetup.setProperty();
//            controllerSetup.setProperty(PutCQL.BATCH_SIZE, "350");
//            controllerSetup.setProperty(PutCQL.DRY_RUN, "false");
//            result = coreTest();
//            assertEquals(ControllerSetup.CompareStatus.CHANGE_ACCESS.name(), result.getAttribute(PutCQL.ATTRIBUTE_COMPARE_STATUS));
//        }
//    }
//
//    @Test
//    public void testBasicRepeat10() {
//
//        HashMap<String, String> attributes = new HashMap<String, String>();
//        String content = "\"colbigint\",\"colint\",\"coltext\",\"colfloat\",\"coldouble\",\"coldate\",\"coltime\",\"coltimestamp\",\"colboolean\",\"coluuid\",\"colsmallint\",\"coltinyint\",\"coltimeuuid\",\"colvarchar\"\n" +
//                "\"0\",\"1064\",\"zeVOKGnORq\",\"627.6811\",\"395.8522407512559\",\"1971-11-12\",\"03:37:15\",\"2000-09-25T22:18:45Z\",\"false\",\"6080071f-4dd1-4ea5-b711-9ad0716e242a\",\"8966\",\"55\",\"f45e58f5-c3b7-11ef-8d19-97ae87be7c54\",\"Tzxsw\"\n" +
//                "\"1\",\"1709\",\"7By0z5QEXh\",\"652.03955\",\"326.9081263857284\",\"2013-12-17\",\"08:43:09\",\"2010-04-27T07:02:27Z\",\"false\",\"7d511666-2f81-41c4-9d5c-a5fa87f7d1c3\",\"24399\",\"38\",\"f45e8006-c3b7-11ef-8d19-172ff8d0d752\",\"exAbN\"\n" +
//                "\"2\",\"6249\",\"UYI6AgkcBt\",\"939.01556\",\"373.48559413289485\",\"1980-11-05\",\"15:44:43\",\"2023-11-24T05:59:12Z\",\"false\",\"dbd35d1b-38d0-49a4-8069-9efd68314dc5\",\"6918\",\"72\",\"f45e8007-c3b7-11ef-8d19-d784fa8af8e3\",\"IjnDb\"\n" +
//                "\"3\",\"6998\",\"lXQ69C5HOZ\",\"715.1224\",\"236.7994939033784\",\"1992-02-01\",\"08:07:34\",\"1998-04-09T23:19:18Z\",\"true\",\"84a7395c-94fd-43f5-84c6-4152f0407e93\",\"22123\",\"39\",\"f45e8008-c3b7-11ef-8d19-0376318d55df\",\"jyZo8\"\n";
//        FlowFile result;
//
//        for (TestSetup controllerSetup: setups) {
//            attributes.put("CQLName", controllerSetup.name);
//
//            for (int i = 0; i < 10; i++) {
//                testRunner.enqueue(content, attributes);
//                controllerSetup.setProperty();
//                controllerSetup.setProperty(PutCQL.BATCH_SIZE, "350");
//                controllerSetup.setProperty(PutCQL.DRY_RUN, "false");
//                result = coreTest();
//                if (i == 0)
//                    assertEquals(ControllerSetup.CompareStatus.CHANGE_ACCESS.name(), result.getAttribute(PutCQL.ATTRIBUTE_COMPARE_STATUS));
//                else
//                    assertEquals(ControllerSetup.CompareStatus.SAME.name(), result.getAttribute(PutCQL.ATTRIBUTE_COMPARE_STATUS));
//            }
//        }
//    }

}