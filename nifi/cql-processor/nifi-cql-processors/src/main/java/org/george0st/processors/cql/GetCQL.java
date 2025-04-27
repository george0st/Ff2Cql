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
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.george0st.cql.CQLClientService;
import org.george0st.processors.cql.helper.SetupRead;
import org.george0st.processors.cql.processor.CsvCqlRead;

import java.io.*;
import java.util.List;
import java.util.Set;

@Tags({"cql", "cassandra", "scylladb", "astradb", "cassandra query language",
        "nosql", "read", "get"})
@CapabilityDescription("Read the contents of CQL engine (support Apache Cassandra, " +
        "ScyllaDB, AstraDB, etc.) to the FlowFile.")
@SeeAlso({})
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({
        @WritesAttribute(attribute=CQLAttributes.READ_COUNT, description=CQLAttributes.READ_COUNT_DESC),
        @WritesAttribute(attribute=CQLAttributes.ERROR_MESSAGE, description=CQLAttributes.ERROR_MESSAGE_DESC)})
public class GetCQL extends AbstractProcessor {

    //  region All Properties

    public static final PropertyDescriptor SERVICE_CONTROLLER = new PropertyDescriptor
            .Builder()
            .name("Service Controller")
            .description("Service controller to CQL.")
            .required(true)
            .identifiesControllerService(CQLClientService.class)
            .build();

    public static final PropertyDescriptor CONSISTENCY_LEVEL = new PropertyDescriptor
            .Builder()
            .name("Consistency Level")
            .description("Read consistency Level for CQL operations.")
            .required(true)
            .defaultValue(CQLClientService.CL_LOCAL_ONE.getValue())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(CQLClientService.CL_LOCAL_ONE, CQLClientService.CL_LOCAL_QUORUM, CQLClientService.CL_LOCAL_SERIAL,
                    CQLClientService.CL_EACH_QUORUM, CQLClientService.CL_ANY, CQLClientService.CL_ONE,
                    CQLClientService.CL_TWO, CQLClientService.CL_THREE, CQLClientService.CL_QUORUM,
                    CQLClientService.CL_ALL, CQLClientService.CL_SERIAL)
            .build();

    public static final PropertyDescriptor TABLE = new PropertyDescriptor
            .Builder()
            .name("Table")
            .description("Table and schema name in CQL (expected format '<schema>.<table>').")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor COLUMN_NAMES = new PropertyDescriptor.Builder()
            .name("Columns to Return")
            .description("A comma-separated list of column names to be used in the query. If your CQL requires "
                    + "special treatment of the names (quoting, e.g.), each name should include such treatment. If no "
                    + "column names are supplied, all columns in the specified table will be returned. NOTE: It is important "
                    + "to use consistent column names for a given table for incremental fetch to work properly.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor WHERE_CLAUSE = new PropertyDescriptor.Builder()
            .name("Additional WHERE clause")
            .description("A custom clause to be added in the WHERE condition when building CQL queries.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor CQL_QUERY = new PropertyDescriptor.Builder()
            .name("Custom Query")
            .displayName("Custom Query")
            .description("A custom CQL query used to retrieve data. Instead of building a CQL query from "
                    + "other properties, this query will be wrapped as a sub-query. Query must have no ORDER BY statement.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    //  TOBE: Future extension for addition properties
    //    Fetch Size
    //    Max Rows Per Flow File
    //    Output Batch Size

    //  endregion All Properties

    //  region All Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success processing")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed processing")
            .build();

    //  endregion All Relationships

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(SERVICE_CONTROLLER,
                CONSISTENCY_LEVEL,
                TABLE,
                COLUMN_NAMES,
                WHERE_CLAUSE,
                CQL_QUERY);
        relationships = Set.of(REL_SUCCESS, REL_FAILURE);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    protected CQLClientService clientService;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        clientService = context.getProperty(SERVICE_CONTROLLER).asControllerService(CQLClientService.class);
    }

    private String getContent(FlowFile flowFile, ProcessSession session){
        final var byteArrayOutputStream = new ByteArrayOutputStream();
        session.exportTo(flowFile, byteArrayOutputStream);
        return byteArrayOutputStream.toString();
    }

    private byte[] getByteContent(FlowFile flowFile, ProcessSession session){
        final var byteArrayOutputStream = new ByteArrayOutputStream();
        session.exportTo(flowFile, byteArrayOutputStream);
        return byteArrayOutputStream.toByteArray();
    }

    /**
     * Update current content file
     *
     * @param flowFile  Current flow file
     * @param session   Current session
     * @param content   Content for write
     */
    private void updateContent(FlowFile flowFile, ProcessSession session, String content){
        InputStream inputStream = new ByteArrayInputStream(content.getBytes());
        session.importFrom(inputStream, flowFile);
    }

    /**
     * Create new content file
     *
     * @param flowFile  Old flow file (null is also valid)
     * @param session   Current session
     * @param content   Content for write
     * @return          New flow file
     */
    private FlowFile writeContent(FlowFile flowFile, ProcessSession session, String content) {
        FlowFile nextFlowFile;

        // only in case, if flow file is null
        if (flowFile == null)
            flowFile = session.create();

        nextFlowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(final OutputStream outputStream) throws IOException {
                outputStream.write(content.getBytes());
                outputStream.flush();
            }
        });
        return nextFlowFile;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null)
            flowFile = session.create();

        try {
            //  1. get cql session (based on controller)
            try (CqlSession cqlSession = clientService.getSession()) {

                //  2. get setting from processor
                CsvCqlRead read = new CsvCqlRead(cqlSession, new SetupRead(context));

                //  3. put data (FlowFile) to CQL
                CsvCqlRead.ReadResult result = read.executeContent();

                //  4. write information to the output (include write attribute)
                session.putAttribute(flowFile, CQLAttributes.READ_COUNT, Long.toString(result.rows));
                writeContent(flowFile, session, result.content);

                //  5. success and provenance reporting
                session.getProvenanceReporter().send(flowFile, clientService.getURI());
                session.transfer(flowFile, REL_SUCCESS);
            }
        }
        catch (InvalidQueryException ex){
            getLogger().error("GetCQL, InvalidQuery error: ", ex);
            flowFile = session.putAttribute(flowFile, CQLAttributes.ERROR_MESSAGE, ex.getMessage());
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
        catch (Exception ex) {
            getLogger().error("GetCQL, Error: ", ex);
            flowFile = session.putAttribute(flowFile, CQLAttributes.ERROR_MESSAGE, ex.getMessage());
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
