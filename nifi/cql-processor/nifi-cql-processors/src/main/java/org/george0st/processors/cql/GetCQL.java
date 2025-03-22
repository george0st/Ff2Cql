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
import org.apache.nifi.processor.util.StandardValidators;
import org.george0st.cql.CQLClientService;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Set;

@Tags({"cql", "cassandra", "scylladb", "astradb", "cassandra query language",
        "nosql", "read", "get"})
@CapabilityDescription("Read the contents of CQL engine (support Apache Cassandra, " +
        "ScyllaDB, AstraDB, etc.) to the FlowFile.")
@SeeAlso({})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({
        @WritesAttribute(attribute = GetCQL.ATTRIBUTE_READ_COUNT, description = "Amount of read rows from CQL.")})
public class GetCQL extends AbstractProcessor {

    static final String ATTRIBUTE_READ_COUNT = "cql.read.count";

    static final AllowableValue BT_LOGGED = new AllowableValue("LOGGED", "LOGGED");
    static final AllowableValue BT_UNLOGGED = new AllowableValue("UNLOGGED", "UNLOGGED");

    //  region All Properties

    public static final PropertyDescriptor SERVICE_CONTROLLER = new PropertyDescriptor
            .Builder()
            .name("Service Controller")
            .description("Service controller to CQL.")
            .required(true)
            .identifiesControllerService(CQLClientService.class)
            .build();

    public static final PropertyDescriptor READ_CONSISTENCY_LEVEL = new PropertyDescriptor
            .Builder()
            .name("Read Consistency Level")
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
//
//    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor
//            .Builder()
//            .name("Batch Size")
//            .description("Size of batch for data ingest (in one operation).")
//            .required(false)
//            .defaultValue("200")
//            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
//            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
//            .build();
//
//    public static final PropertyDescriptor BATCH_TYPE = new PropertyDescriptor
//            .Builder()
//            .name("Batch Type")
//            .description("Batch type with relation to an atomicity of batch operation.")
//            .required(false)
//            .defaultValue(BT_UNLOGGED)
//            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
//            .allowableValues(BT_UNLOGGED, BT_LOGGED)
//            .build();
//
//    public static final PropertyDescriptor DRY_RUN = new PropertyDescriptor
//            .Builder()
//            .name("Dry Run")
//            .description("Dry run for processing (without final write to CQL engine).")
//            .required(false)
//            .defaultValue("false")
//            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
//            .allowableValues("true", "false")
//            .build();

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
                READ_CONSISTENCY_LEVEL);
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

    private void updateContent(FlowFile flowFile, ProcessSession session, String content){
        InputStream inputStream = new ByteArrayInputStream(content.getBytes());
        session.importFrom(inputStream, flowFile);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) return;
//
//        try {
//            //  1. get cql session (based on controller)
//            try (CqlSession cqlSession = clientService.getSession()) {
//
//                //  2. get setting from processor
//                CsvCqlWrite write = new CsvCqlWrite(cqlSession, new SetupWrite(context));
//
//                //  3. put data (FlowFile) to CQL
//                long count = write.executeContent(this.getByteContent(flowFile, session));
//
//                //  4. write some information to the output (as write attributes)
//                session.putAttribute(flowFile, ATTRIBUTE_COUNT, Long.toString(count));
//
//                //  5. success and provenance reporting
//                session.getProvenanceReporter().send(flowFile, clientService.getURI());
//                session.transfer(flowFile, REL_SUCCESS);
//            }
//        }
//        catch (InvalidQueryException ex){
//            getLogger().error("PutCQL, OnTrigger: InvalidQuery error", ex);
//            session.transfer(flowFile, REL_FAILURE);
//        }
//        catch (Exception ex) {
//            getLogger().error("PutCQL, OnTrigger: Error", ex);
//            session.transfer(flowFile, REL_FAILURE);
//        }
        session.getProvenanceReporter().send(flowFile, clientService.getURI());
        session.transfer(flowFile, REL_SUCCESS);
    }
}
