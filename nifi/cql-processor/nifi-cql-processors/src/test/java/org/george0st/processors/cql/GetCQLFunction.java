package org.george0st.processors.cql;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.george0st.processors.cql.helper.TestSetupRead;
import org.george0st.processors.cql.helper.TestSetupWrite;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class GetCQLFunction extends GetCQLBase {

    public GetCQLFunction() throws Exception {
        super();
    }

    @Test
    public void testGetWithEmptyContent() throws Exception {
        MockFlowFile result;
        String resultContent;

        // Read data
        for (TestSetupRead setup : setups) {
            setup.columnNames="colbigint, colint";
            setup.whereClause="colbigint=3";

            result = runTest(setup);
            resultContent = result.getContent();

            //  check amount of write items
            assertNotNull(result, String.format("Issue with processing in '%s'", setup.name));
            assertEquals(4, Long.parseLong(result.getAttribute(CQLAttributes.COUNT)));
            assertTrue(resultContent.indexOf("6998")!=-1,"Invalid output");
        }

    }

    @Test
    public void testGetWithSomeContent() throws Exception {
        String content = "\"colbigint\",\"colint\",\"coltext\",\"colfloat\",\"coldouble\",\"coldate\",\"coltime\",\"coltimestamp\",\"colboolean\",\"coluuid\",\"colsmallint\",\"coltinyint\",\"coltimeuuid\",\"colvarchar\"\n" +
                "\"0\",\"1064\",\"zeVOKGnORq\",\"627.6811\",\"395.8522407512559\",\"1971-11-12\",\"03:37:15\",\"2000-09-25T22:18:45Z\",\"false\",\"6080071f-4dd1-4ea5-b711-9ad0716e242a\",\"8966\",\"55\",\"f45e58f5-c3b7-11ef-8d19-97ae87be7c54\",\"Tzxsw\"\n" +
                "\"1\",\"1709\",\"7By0z5QEXh\",\"652.03955\",\"326.9081263857284\",\"2013-12-17\",\"08:43:09\",\"2010-04-27T07:02:27Z\",\"false\",\"7d511666-2f81-41c4-9d5c-a5fa87f7d1c3\",\"24399\",\"38\",\"f45e8006-c3b7-11ef-8d19-172ff8d0d752\",\"exAbN\"\n" +
                "\"2\",\"6249\",\"UYI6AgkcBt\",\"939.01556\",\"373.48559413289485\",\"1980-11-05\",\"15:44:43\",\"2023-11-24T05:59:12Z\",\"false\",\"dbd35d1b-38d0-49a4-8069-9efd68314dc5\",\"6918\",\"72\",\"f45e8007-c3b7-11ef-8d19-d784fa8af8e3\",\"IjnDb\"\n" +
                "\"3\",\"6998\",\"lXQ69C5HOZ\",\"715.1224\",\"236.7994939033784\",\"1992-02-01\",\"08:07:34\",\"2024-06-29T21:08:54.463Z\",\"true\",\"84a7395c-94fd-43f5-84c6-4152f0407e93\",\"22123\",\"39\",\"f45e8008-c3b7-11ef-8d19-0376318d55df\",\"jyZo8\"\n";
        MockFlowFile result;
        String resultContent;

//        PutCQLFunction putCQL=new PutCQLFunction();
//        putCQL.testBasic();

        for (TestSetupRead setup : setups) {
            setup.columnNames="colbigint, colint";
            setup.whereClause="colbigint=3";

            result = runTest(setup, content);
            resultContent = result.getContent();

            //  check amount of write items
            assertNotNull(result, String.format("Issue with processing in '%s'", setup.name));
            assertEquals(4, Long.parseLong(result.getAttribute(CQLAttributes.COUNT)));
            assertEquals(resultContent, "212");
        }

    }

}
