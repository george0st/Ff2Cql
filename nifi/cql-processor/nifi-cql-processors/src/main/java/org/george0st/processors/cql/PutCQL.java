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
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.george0st.processors.cql.helper.Setup;
import org.george0st.processors.cql.processor.CsvCqlWrite;

import java.io.*;
import java.util.List;
import java.util.Set;

@Tags({"CQL", "Cassandra", "ScyllaDB", "AstraDB", "YugabyteDB", "Cassandra Query Language",
        "NoSQL", "Write", "Insert", "Update", "Put"})
@CapabilityDescription("Writes the contents of FlowFile to an CQL engine (support Apache Cassandra, " +
        "ScyllaDB, AstraDB, etc.). The processor expects content in FlowFile/CSV with header.")
@SeeAlso({})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({
        @WritesAttribute(attribute=PutCQL.ATTRIBUTE_COUNT, description="Amount of write rows to CQL."),
        @WritesAttribute(attribute=PutCQL.ATTRIBUTE_COMPARE_STATUS, description="View to the internal CQL processing.")})
public class PutCQL extends AbstractProcessor {

    static final String ATTRIBUTE_COUNT = "CQLCount";
    static final String ATTRIBUTE_COMPARE_STATUS = "CQLCompareStatus";

    static final AllowableValue CL_LOCAL_ONE = new AllowableValue("LOCAL_ONE", "Local one");
    static final AllowableValue CL_LOCAL_QUORUM = new AllowableValue("LOCAL_QUORUM", "Local quorum");
    static final AllowableValue CL_LOCAL_SERIAL = new AllowableValue("LOCAL_SERIAL", "Local serial");
    static final AllowableValue CL_EACH_QUORUM = new AllowableValue("EACH_QUORUM", "Each quorum");
    static final AllowableValue CL_ANY = new AllowableValue("ANY", "Any");
    static final AllowableValue CL_ONE = new AllowableValue("ONE", "One");
    static final AllowableValue CL_TWO = new AllowableValue("TWO", "Two");
    static final AllowableValue CL_THREE = new AllowableValue("THREE", "Three");
    static final AllowableValue CL_QUORUM = new AllowableValue("QUORUM", "Quorum");
    static final AllowableValue CL_ALL = new AllowableValue("ALL", "All");
    static final AllowableValue CL_SERIAL = new AllowableValue("SERIAL", "Serial");

    //  region All Properties

    public static final PropertyDescriptor MY_IP_ADDRESSES = new PropertyDescriptor
            .Builder()
            .name("IP Addresses")
            .description("List of IP addresses for CQL connection, the addresses are split by comma (e.g. '192.168.0.1, 192.168.0.2').")
            .required(true)
            .defaultValue("localhost")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MY_PORT = new PropertyDescriptor
            .Builder()
            .name("Port")
            .description("Port for communication.")
            .required(true)
            .defaultValue("9042")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor MY_USERNAME = new PropertyDescriptor
            .Builder()
            .name("Username")
            .description("Username for the CQL connection.")
            .required(false)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
            //.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MY_PASSWORD = new PropertyDescriptor
            .Builder()
            .name("Password")
            .description("Password for the CQL connection.")
            .required(false)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
            //.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor MY_LOCALDC = new PropertyDescriptor
            .Builder()
            .name("Local Data Center")
            .description("Name of local data center e.g. 'dc1', 'datacenter1', etc.")
            .required(false)
            .defaultValue("dc1")
            .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
            //.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MY_CONNECTION_TIMEOUT = new PropertyDescriptor
            .Builder()
            .name("Connection Timeout")
            .description("Timeout for connection to CQL engine.")
            .required(true)
            .defaultValue("900")
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();

    public static final PropertyDescriptor MY_REQUEST_TIMEOUT = new PropertyDescriptor
            .Builder()
            .name("Request Timeout")
            .description("Timeout for request to CQL engine.")
            .required(true)
            .defaultValue("60")
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();

    public static final PropertyDescriptor MY_CONSISTENCY_LEVEL = new PropertyDescriptor
            .Builder()
            .name("Consistency Level")
            .description("Consistency Level for CQL operations.")
            .required(true)
            .defaultValue(CL_LOCAL_ONE.getValue())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(CL_LOCAL_ONE, CL_LOCAL_QUORUM, CL_LOCAL_SERIAL, CL_EACH_QUORUM, CL_ANY, CL_ONE, CL_TWO, CL_THREE, CL_QUORUM, CL_ALL, CL_SERIAL)
            .build();

    public static final PropertyDescriptor MY_TABLE = new PropertyDescriptor
            .Builder()
            .name("Table")
            .description("Table and schema name in CQL (expected format <schema>.<table>).")
            .required(true)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MY_BATCH_SIZE = new PropertyDescriptor
            .Builder()
            .name("Batch Size")
            .description("Size of bulk for data ingest.")
            .required(false)
            .defaultValue("200")
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .build();

    public static final PropertyDescriptor MY_DRY_RUN = new PropertyDescriptor
            .Builder()
            .name("Dry Run")
            .description("Dry run for processing (without final write to CQL engine).")
            .required(false)
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .build();

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

    private Setup setup = null;
    private CqlAccess cqlAccess;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(MY_IP_ADDRESSES,
                MY_PORT,
                MY_USERNAME,
                MY_PASSWORD,
                MY_LOCALDC,
                MY_CONNECTION_TIMEOUT,
                MY_REQUEST_TIMEOUT,
                MY_CONSISTENCY_LEVEL,
                MY_TABLE,
                MY_BATCH_SIZE,
                MY_DRY_RUN);
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

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
    }

    /**
     * Get flow file content
     * @param flowFile  The Flow file
     * @param session   The client session
     * @return The flow file as string
     */
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
     * Set flow file content based on string
     * @param flowFile  The flow file
     * @param session   The client session
     * @param content   The content for write to flow file
     */
    private void updateContent(FlowFile flowFile, ProcessSession session, String content){
        InputStream inputStream = new ByteArrayInputStream(content.getBytes());
        session.importFrom(inputStream, flowFile);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        //  1. create/get data for connection
        try {
            Setup newSetup = new Setup(context);

            synchronized (this) {
                Setup.CompareStatus status;

                if ((status=newSetup.compare(setup)) != Setup.CompareStatus.SAME) {
                    setup = newSetup;
                    if (status == Setup.CompareStatus.CHANGE_ACCESS)
                        cqlAccess = new CqlAccess(setup);
                }
                session.putAttribute(flowFile, "CQLCompareStatus", status.name());
            }

            //  2. get data from FlowFile (support CSV)
            CsvCqlWrite write=new CsvCqlWrite(cqlAccess, setup.dryRun);

            //  3. put data to CQL
            long count=write.executeContent(this.getByteContent(flowFile,session));

            //  4. write some information to the output (as write attributes)
            session.putAttribute(flowFile, "CQLCount", Long.toString(count));
        } catch (IOException e) {
            getLogger().error("CQLProcessor, Processing error", e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
        session.transfer(flowFile, REL_SUCCESS);
    }
}
