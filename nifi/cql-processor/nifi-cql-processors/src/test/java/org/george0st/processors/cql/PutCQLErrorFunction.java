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
import org.apache.nifi.reporting.InitializationException;
import org.george0st.processors.cql.helper.TestSetupWrite;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class PutCQLErrorFunction extends PutCQLBase {

    public PutCQLErrorFunction() throws IOException, InterruptedException, InitializationException {
        super();
    }

    @Test
    public void testErrorNonExistColumnInCSV() throws Exception {
        String content = "aaa\n" +
                "0\n" +
                "1\n";
        FlowFile result;

        for (TestSetupWrite setup: setups) {
            result = runTest(setup, content);
            assertNull(result, String.format("Expected null, besed on simulation error in '%s'", setup.name));
        }
    }

    @Test
    public void testErrorMissingPrimaryKeyColumnInCSV() throws Exception {
        String content = "colbigint\n" +
                "0\n" +
                "1\n";
        FlowFile result;

        for (TestSetupWrite setup: setups) {
            result = runTest(setup, content);
            assertNull(result, String.format("Expected null, based on simulation error in '%s'", setup.name));
        }
    }

    @Test
    public void testErrorInvalidIntTypeValueInCSV() throws Exception {
        String content = "colbigint,colint\n" +
                "0,Peter\n" +
                "1,John\n";
        FlowFile result;

        for (TestSetupWrite setup: setups) {
            result = runTest(setup, content);
            assertNull(result, String.format("Expected null, based on simulation error in '%s'", setup.name));
        }
    }

    @Test
    public void testErrorInvalidFloatTypeValueInCSV() throws Exception {
        String content = "colbigint,colint,colfloat\n" +
                "0,1064,Peter\n" +
                "1,1709,John\n";
        FlowFile result;

        for (TestSetupWrite setup: setups) {
            result = runTest(setup, content);
            assertNull(result, String.format("Expected null, based on simulation error in '%s'", setup.name));
        }
    }

}