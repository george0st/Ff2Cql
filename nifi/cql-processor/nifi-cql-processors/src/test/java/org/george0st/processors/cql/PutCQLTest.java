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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.george0st.processors.cql.helper.ReadableValue;
import org.george0st.processors.cql.helper.Setup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import java.io.IOException;
import java.util.HashMap;

public class PutCQLTest {

    private TestRunner testRunner;
    private TestProperty testProperty;

    // Helper
    // https://medium.com/@mr.sinchan.banerjee/nifi-custom-processor-series-part-3-junit-test-with-nifi-mock-a935a1a4e3e5

    @BeforeEach
    public void init() throws IOException {
        testProperty = TestProperty.getInstance(TestProperty.getTestPropertyFile(
                new String []{"test-properties-private.json", "test-properties.json"}));
        testRunner = TestRunners.newTestRunner(PutCQL.class);
    }

    private FlowFile coreTest(){
        long finish, start, count;
        FlowFile result;

        //"10.129.53.159,10.129.53.154,10.129.53.153"
        testRunner.setProperty(PutCQL.MY_IP_ADDRESSES.getName(), String.join(",", testProperty.ipAddresses));
        testRunner.setProperty(PutCQL.MY_LOCALDC.getName(),testProperty.localDC);
        testRunner.setProperty(PutCQL.MY_USERNAME.getName(), testProperty.username);
        testRunner.setProperty(PutCQL.MY_PASSWORD.getName(), testProperty.pwd);
        testRunner.setProperty(PutCQL.MY_TABLE.getName(), testProperty.table);

        start = System.currentTimeMillis();
        testRunner.run();
        result = testRunner.getFlowFilesForRelationship(PutCQL.REL_SUCCESS).getLast();
        finish = System.currentTimeMillis();

        count=Long.parseLong(result.getAttribute("CQLCount"));
        System.out.printf("'%s': %s (%d ms); Access: %s; Items: %d; Perf: %.1f [calls/sec]%s",
                "FlowFile",
                ReadableValue.fromMillisecond(finish - start),
                finish-start,
                result.getAttribute("CQLCompareStatus"),
                count,
                count / ((finish - start) / 1000.0),
                System.lineSeparator());
        return result;
    }

    @Test
    public void testBasic() {

        HashMap<String, String> attributes = new HashMap<String, String>() {{
            put("xxxx", "yyyy");
            put("aa", "bb");
        }};

        String content = "\"colbigint\",\"colint\",\"coltext\",\"colfloat\",\"coldouble\",\"coldate\",\"coltime\",\"coltimestamp\",\"colboolean\",\"coluuid\",\"colsmallint\",\"coltinyint\",\"coltimeuuid\",\"colvarchar\"\n" +
                "\"0\",\"1064\",\"zeVOKGnORq\",\"627.6811\",\"395.8522407512559\",\"1971-11-12\",\"03:37:15\",\"2000-09-25T22:18:45Z\",\"false\",\"6080071f-4dd1-4ea5-b711-9ad0716e242a\",\"8966\",\"55\",\"f45e58f5-c3b7-11ef-8d19-97ae87be7c54\",\"Tzxsw\"\n" +
                "\"1\",\"1709\",\"7By0z5QEXh\",\"652.03955\",\"326.9081263857284\",\"2013-12-17\",\"08:43:09\",\"2010-04-27T07:02:27Z\",\"false\",\"7d511666-2f81-41c4-9d5c-a5fa87f7d1c3\",\"24399\",\"38\",\"f45e8006-c3b7-11ef-8d19-172ff8d0d752\",\"exAbN\"\n" +
                "\"2\",\"6249\",\"UYI6AgkcBt\",\"939.01556\",\"373.48559413289485\",\"1980-11-05\",\"15:44:43\",\"2023-11-24T05:59:12Z\",\"false\",\"dbd35d1b-38d0-49a4-8069-9efd68314dc5\",\"6918\",\"72\",\"f45e8007-c3b7-11ef-8d19-d784fa8af8e3\",\"IjnDb\"\n" +
                "\"3\",\"6998\",\"lXQ69C5HOZ\",\"715.1224\",\"236.7994939033784\",\"1992-02-01\",\"08:07:34\",\"1998-04-09T23:19:18Z\",\"true\",\"84a7395c-94fd-43f5-84c6-4152f0407e93\",\"22123\",\"39\",\"f45e8008-c3b7-11ef-8d19-0376318d55df\",\"jyZo8\"\n";
        FlowFile result;

        testRunner.enqueue(content, attributes);
        testRunner.setProperty(PutCQL.MY_BATCH_SIZE.getName(), "350");
        testRunner.setProperty(PutCQL.MY_DRY_RUN.getName(), "false");

        result=coreTest();
        assertEquals(Setup.CompareStatus.CHANGE_ACCESS.name(), result.getAttribute("CQLCompareStatus"));
    }

    @Test
    public void testBasic2ItemsSameSetup() {

        HashMap<String, String> attributes = new HashMap<String, String>() {{
            put("xxxx", "yyyy");
            put("aa", "bb");
        }};

        String content = "\"colbigint\",\"colint\",\"coltext\",\"colfloat\",\"coldouble\",\"coldate\",\"coltime\",\"coltimestamp\",\"colboolean\",\"coluuid\",\"colsmallint\",\"coltinyint\",\"coltimeuuid\",\"colvarchar\"\n" +
                "\"0\",\"1064\",\"zeVOKGnORq\",\"627.6811\",\"395.8522407512559\",\"1971-11-12\",\"03:37:15\",\"2000-09-25T22:18:45Z\",\"false\",\"6080071f-4dd1-4ea5-b711-9ad0716e242a\",\"8966\",\"55\",\"f45e58f5-c3b7-11ef-8d19-97ae87be7c54\",\"Tzxsw\"\n" +
                "\"1\",\"1709\",\"7By0z5QEXh\",\"652.03955\",\"326.9081263857284\",\"2013-12-17\",\"08:43:09\",\"2010-04-27T07:02:27Z\",\"false\",\"7d511666-2f81-41c4-9d5c-a5fa87f7d1c3\",\"24399\",\"38\",\"f45e8006-c3b7-11ef-8d19-172ff8d0d752\",\"exAbN\"\n" +
                "\"2\",\"6249\",\"UYI6AgkcBt\",\"939.01556\",\"373.48559413289485\",\"1980-11-05\",\"15:44:43\",\"2023-11-24T05:59:12Z\",\"false\",\"dbd35d1b-38d0-49a4-8069-9efd68314dc5\",\"6918\",\"72\",\"f45e8007-c3b7-11ef-8d19-d784fa8af8e3\",\"IjnDb\"\n" +
                "\"3\",\"6998\",\"lXQ69C5HOZ\",\"715.1224\",\"236.7994939033784\",\"1992-02-01\",\"08:07:34\",\"1998-04-09T23:19:18Z\",\"true\",\"84a7395c-94fd-43f5-84c6-4152f0407e93\",\"22123\",\"39\",\"f45e8008-c3b7-11ef-8d19-0376318d55df\",\"jyZo8\"\n";

        String content2 = "\"colbigint\",\"colint\",\"coltext\",\"colfloat\",\"coldouble\",\"coldate\",\"coltime\",\"coltimestamp\",\"colboolean\",\"coluuid\",\"colsmallint\",\"coltinyint\",\"coltimeuuid\",\"colvarchar\"\n" +
                "\"10\",\"1064\",\"zeVOKGnORq\",\"627.6811\",\"395.8522407512559\",\"1971-11-12\",\"03:37:15\",\"2000-09-25T22:18:45Z\",\"false\",\"6080071f-4dd1-4ea5-b711-9ad0716e242a\",\"8966\",\"55\",\"f45e58f5-c3b7-11ef-8d19-97ae87be7c54\",\"Tzxsw\"\n" +
                "\"11\",\"1709\",\"7By0z5QEXh\",\"652.03955\",\"326.9081263857284\",\"2013-12-17\",\"08:43:09\",\"2010-04-27T07:02:27Z\",\"false\",\"7d511666-2f81-41c4-9d5c-a5fa87f7d1c3\",\"24399\",\"38\",\"f45e8006-c3b7-11ef-8d19-172ff8d0d752\",\"exAbN\"\n" +
                "\"12\",\"6249\",\"UYI6AgkcBt\",\"939.01556\",\"373.48559413289485\",\"1980-11-05\",\"15:44:43\",\"2023-11-24T05:59:12Z\",\"false\",\"dbd35d1b-38d0-49a4-8069-9efd68314dc5\",\"6918\",\"72\",\"f45e8007-c3b7-11ef-8d19-d784fa8af8e3\",\"IjnDb\"\n" +
                "\"13\",\"6998\",\"lXQ69C5HOZ\",\"715.1224\",\"236.7994939033784\",\"1992-02-01\",\"08:07:34\",\"1998-04-09T23:19:18Z\",\"true\",\"84a7395c-94fd-43f5-84c6-4152f0407e93\",\"22123\",\"39\",\"f45e8008-c3b7-11ef-8d19-0376318d55df\",\"jyZo8\"\n";

        FlowFile result;

        testRunner.enqueue(content, attributes);
        testRunner.setProperty(PutCQL.MY_BATCH_SIZE.getName(), "350");
        testRunner.setProperty(PutCQL.MY_DRY_RUN.getName(), "false");
        result=coreTest();
        assertEquals(Setup.CompareStatus.CHANGE_ACCESS.name(), result.getAttribute("CQLCompareStatus"));

        testRunner.enqueue(content2, attributes);
        testRunner.setProperty(PutCQL.MY_BATCH_SIZE.getName(), "350");
        testRunner.setProperty(PutCQL.MY_DRY_RUN.getName(), "false");
        result=coreTest();
        assertEquals(Setup.CompareStatus.SAME.name(), result.getAttribute("CQLCompareStatus"));
    }

    @Test
    public void testBasic3ItemsDifferentSetup() {

        HashMap<String, String> attributes = new HashMap<String, String>() {{
            put("xxxx", "yyyy");
            put("aa", "bb");
        }};

        String content = "\"colbigint\",\"colint\",\"coltext\",\"colfloat\",\"coldouble\",\"coldate\",\"coltime\",\"coltimestamp\",\"colboolean\",\"coluuid\",\"colsmallint\",\"coltinyint\",\"coltimeuuid\",\"colvarchar\"\n" +
                "\"0\",\"1064\",\"zeVOKGnORq\",\"627.6811\",\"395.8522407512559\",\"1971-11-12\",\"03:37:15\",\"2000-09-25T22:18:45Z\",\"false\",\"6080071f-4dd1-4ea5-b711-9ad0716e242a\",\"8966\",\"55\",\"f45e58f5-c3b7-11ef-8d19-97ae87be7c54\",\"Tzxsw\"\n" +
                "\"1\",\"1709\",\"7By0z5QEXh\",\"652.03955\",\"326.9081263857284\",\"2013-12-17\",\"08:43:09\",\"2010-04-27T07:02:27Z\",\"false\",\"7d511666-2f81-41c4-9d5c-a5fa87f7d1c3\",\"24399\",\"38\",\"f45e8006-c3b7-11ef-8d19-172ff8d0d752\",\"exAbN\"\n" +
                "\"2\",\"6249\",\"UYI6AgkcBt\",\"939.01556\",\"373.48559413289485\",\"1980-11-05\",\"15:44:43\",\"2023-11-24T05:59:12Z\",\"false\",\"dbd35d1b-38d0-49a4-8069-9efd68314dc5\",\"6918\",\"72\",\"f45e8007-c3b7-11ef-8d19-d784fa8af8e3\",\"IjnDb\"\n" +
                "\"3\",\"6998\",\"lXQ69C5HOZ\",\"715.1224\",\"236.7994939033784\",\"1992-02-01\",\"08:07:34\",\"1998-04-09T23:19:18Z\",\"true\",\"84a7395c-94fd-43f5-84c6-4152f0407e93\",\"22123\",\"39\",\"f45e8008-c3b7-11ef-8d19-0376318d55df\",\"jyZo8\"\n";

        String content2 = "\"colbigint\",\"colint\",\"coltext\",\"colfloat\",\"coldouble\",\"coldate\",\"coltime\",\"coltimestamp\",\"colboolean\",\"coluuid\",\"colsmallint\",\"coltinyint\",\"coltimeuuid\",\"colvarchar\"\n" +
                "\"10\",\"1064\",\"zeVOKGnORq\",\"627.6811\",\"395.8522407512559\",\"1971-11-12\",\"03:37:15\",\"2000-09-25T22:18:45Z\",\"false\",\"6080071f-4dd1-4ea5-b711-9ad0716e242a\",\"8966\",\"55\",\"f45e58f5-c3b7-11ef-8d19-97ae87be7c54\",\"Tzxsw\"\n" +
                "\"11\",\"1709\",\"7By0z5QEXh\",\"652.03955\",\"326.9081263857284\",\"2013-12-17\",\"08:43:09\",\"2010-04-27T07:02:27Z\",\"false\",\"7d511666-2f81-41c4-9d5c-a5fa87f7d1c3\",\"24399\",\"38\",\"f45e8006-c3b7-11ef-8d19-172ff8d0d752\",\"exAbN\"\n" +
                "\"12\",\"6249\",\"UYI6AgkcBt\",\"939.01556\",\"373.48559413289485\",\"1980-11-05\",\"15:44:43\",\"2023-11-24T05:59:12Z\",\"false\",\"dbd35d1b-38d0-49a4-8069-9efd68314dc5\",\"6918\",\"72\",\"f45e8007-c3b7-11ef-8d19-d784fa8af8e3\",\"IjnDb\"\n" +
                "\"13\",\"6998\",\"lXQ69C5HOZ\",\"715.1224\",\"236.7994939033784\",\"1992-02-01\",\"08:07:34\",\"1998-04-09T23:19:18Z\",\"true\",\"84a7395c-94fd-43f5-84c6-4152f0407e93\",\"22123\",\"39\",\"f45e8008-c3b7-11ef-8d19-0376318d55df\",\"jyZo8\"\n";
        FlowFile result;

        testRunner.enqueue(content, attributes);
        testRunner.setProperty(PutCQL.MY_BATCH_SIZE.getName(), "350");
        testRunner.setProperty(PutCQL.MY_DRY_RUN.getName(), "false");
        result=coreTest();
        assertEquals(Setup.CompareStatus.CHANGE_ACCESS.name(), result.getAttribute("CQLCompareStatus"));

        testRunner.enqueue(content, attributes);
        testRunner.setProperty(PutCQL.MY_BATCH_SIZE.getName(), "350");
        testRunner.setProperty(PutCQL.MY_DRY_RUN.getName(), "false");
        result=coreTest();
        assertEquals(Setup.CompareStatus.SAME.name(), result.getAttribute("CQLCompareStatus"));

        testRunner.enqueue(content2, attributes);
        testRunner.setProperty(PutCQL.MY_BATCH_SIZE.getName(), "150");
        testRunner.setProperty(PutCQL.MY_DRY_RUN.getName(), "false");
        result=coreTest();
        assertEquals(Setup.CompareStatus.CHANGE.name(), result.getAttribute("CQLCompareStatus"));
    }

    @Test
    public void testBasic4ItemsSameSetup() {

        String content = "\"colbigint\",\"colint\",\"coltext\",\"colfloat\",\"coldouble\",\"coldate\",\"coltime\",\"coltimestamp\",\"colboolean\",\"coluuid\",\"colsmallint\",\"coltinyint\",\"coltimeuuid\",\"colvarchar\"\n" +
                "\"0\",\"1064\",\"zeVOKGnORq\",\"627.6811\",\"395.8522407512559\",\"1971-11-12\",\"03:37:15\",\"2000-09-25T22:18:45Z\",\"false\",\"6080071f-4dd1-4ea5-b711-9ad0716e242a\",\"8966\",\"55\",\"f45e58f5-c3b7-11ef-8d19-97ae87be7c54\",\"Tzxsw\"\n" +
                "\"1\",\"1709\",\"7By0z5QEXh\",\"652.03955\",\"326.9081263857284\",\"2013-12-17\",\"08:43:09\",\"2010-04-27T07:02:27Z\",\"false\",\"7d511666-2f81-41c4-9d5c-a5fa87f7d1c3\",\"24399\",\"38\",\"f45e8006-c3b7-11ef-8d19-172ff8d0d752\",\"exAbN\"\n" +
                "\"2\",\"6249\",\"UYI6AgkcBt\",\"939.01556\",\"373.48559413289485\",\"1980-11-05\",\"15:44:43\",\"2023-11-24T05:59:12Z\",\"false\",\"dbd35d1b-38d0-49a4-8069-9efd68314dc5\",\"6918\",\"72\",\"f45e8007-c3b7-11ef-8d19-d784fa8af8e3\",\"IjnDb\"\n" +
                "\"3\",\"6998\",\"lXQ69C5HOZ\",\"715.1224\",\"236.7994939033784\",\"1992-02-01\",\"08:07:34\",\"1998-04-09T23:19:18Z\",\"true\",\"84a7395c-94fd-43f5-84c6-4152f0407e93\",\"22123\",\"39\",\"f45e8008-c3b7-11ef-8d19-0376318d55df\",\"jyZo8\"\n";

        FlowFile result;

        testRunner.enqueue(content);
        testRunner.setProperty(PutCQL.MY_BATCH_SIZE.getName(), "350");
        testRunner.setProperty(PutCQL.MY_DRY_RUN.getName(), "false");
        result=coreTest();
        assertEquals(Setup.CompareStatus.CHANGE_ACCESS.name(), result.getAttribute("CQLCompareStatus"));

        testRunner.enqueue(content);
        testRunner.setProperty(PutCQL.MY_BATCH_SIZE.getName(), "350");
        testRunner.setProperty(PutCQL.MY_DRY_RUN.getName(), "false");
        result=coreTest();
        assertEquals(Setup.CompareStatus.SAME.name(), result.getAttribute("CQLCompareStatus"));

        testRunner.enqueue(content);
        testRunner.setProperty(PutCQL.MY_BATCH_SIZE.getName(), "350");
        testRunner.setProperty(PutCQL.MY_DRY_RUN.getName(), "false");
        result=coreTest();
        assertEquals(Setup.CompareStatus.SAME.name(), result.getAttribute("CQLCompareStatus"));

        testRunner.enqueue(content);
        testRunner.setProperty(PutCQL.MY_BATCH_SIZE.getName(), "350");
        testRunner.setProperty(PutCQL.MY_DRY_RUN.getName(), "false");
        result=coreTest();
        assertEquals(Setup.CompareStatus.SAME.name(), result.getAttribute("CQLCompareStatus"));
    }

    @Test
    public void testEmptyInput() {
        String content = "";
        FlowFile result;

        testRunner.enqueue(content);
        testRunner.setProperty(PutCQL.MY_BATCH_SIZE.getName(), "350");
        testRunner.setProperty(PutCQL.MY_DRY_RUN.getName(), "false");
        result=coreTest();
        assertEquals(Setup.CompareStatus.CHANGE_ACCESS.name(), result.getAttribute("CQLCompareStatus"));
    }

    @Test
    public void testOnlyHeader() {
        String content = "\"colbigint\",\"colint\",\"coltext\",\"colfloat\",\"coldouble\",\"coldate\",\"coltime\",\"coltimestamp\",\"colboolean\",\"coluuid\",\"colsmallint\",\"coltinyint\",\"coltimeuuid\",\"colvarchar\"\n";
        FlowFile result;

        testRunner.enqueue(content);
        testRunner.setProperty(PutCQL.MY_BATCH_SIZE.getName(), "350");
        testRunner.setProperty(PutCQL.MY_DRY_RUN.getName(), "false");
        result=coreTest();
        assertEquals(Setup.CompareStatus.CHANGE_ACCESS.name(), result.getAttribute("CQLCompareStatus"));
    }

    @Test
    public void testBasicRepeat10() {

        String content = "\"colbigint\",\"colint\",\"coltext\",\"colfloat\",\"coldouble\",\"coldate\",\"coltime\",\"coltimestamp\",\"colboolean\",\"coluuid\",\"colsmallint\",\"coltinyint\",\"coltimeuuid\",\"colvarchar\"\n" +
                "\"0\",\"1064\",\"zeVOKGnORq\",\"627.6811\",\"395.8522407512559\",\"1971-11-12\",\"03:37:15\",\"2000-09-25T22:18:45Z\",\"false\",\"6080071f-4dd1-4ea5-b711-9ad0716e242a\",\"8966\",\"55\",\"f45e58f5-c3b7-11ef-8d19-97ae87be7c54\",\"Tzxsw\"\n" +
                "\"1\",\"1709\",\"7By0z5QEXh\",\"652.03955\",\"326.9081263857284\",\"2013-12-17\",\"08:43:09\",\"2010-04-27T07:02:27Z\",\"false\",\"7d511666-2f81-41c4-9d5c-a5fa87f7d1c3\",\"24399\",\"38\",\"f45e8006-c3b7-11ef-8d19-172ff8d0d752\",\"exAbN\"\n" +
                "\"2\",\"6249\",\"UYI6AgkcBt\",\"939.01556\",\"373.48559413289485\",\"1980-11-05\",\"15:44:43\",\"2023-11-24T05:59:12Z\",\"false\",\"dbd35d1b-38d0-49a4-8069-9efd68314dc5\",\"6918\",\"72\",\"f45e8007-c3b7-11ef-8d19-d784fa8af8e3\",\"IjnDb\"\n" +
                "\"3\",\"6998\",\"lXQ69C5HOZ\",\"715.1224\",\"236.7994939033784\",\"1992-02-01\",\"08:07:34\",\"1998-04-09T23:19:18Z\",\"true\",\"84a7395c-94fd-43f5-84c6-4152f0407e93\",\"22123\",\"39\",\"f45e8008-c3b7-11ef-8d19-0376318d55df\",\"jyZo8\"\n";
        FlowFile result;

        for (int i=0; i<10;i++) {
            testRunner.enqueue(content);
            testRunner.setProperty(PutCQL.MY_BATCH_SIZE.getName(), "350");
            testRunner.setProperty(PutCQL.MY_DRY_RUN.getName(), "false");

            result = coreTest();
            if (i==0)
                assertEquals(Setup.CompareStatus.CHANGE_ACCESS.name(), result.getAttribute("CQLCompareStatus"));
            else
                assertEquals(Setup.CompareStatus.SAME.name(), result.getAttribute("CQLCompareStatus"));
        }
    }

}