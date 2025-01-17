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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.junit.jupiter.api.*;

import java.util.HashMap;

public class CqlProcessorTest {

    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(CqlProcessor.class);
    }

    @Test
    public void testProcessor() {

        String params = "{\"testtt\":\"value\"}";
        HashMap<String, String> attributes= new HashMap<String, String>(){{
            put("abc.param", params);
        }};

        String content="";
        testRunner.enqueue(content,attributes);
        testRunner.setProperty("Batch Size","350");
        testRunner.setProperty("Dry Run", "false");
        testRunner.run();

        FlowFile result = testRunner.getFlowFilesForRelationship(CqlProcessor.REL_SUCCESS).get(0);

        // https://medium.com/@mr.sinchan.banerjee/nifi-custom-processor-series-part-3-junit-test-with-nifi-mock-a935a1a4e3e5
        // TODO: add assert
        //testRunner.

    }

}
