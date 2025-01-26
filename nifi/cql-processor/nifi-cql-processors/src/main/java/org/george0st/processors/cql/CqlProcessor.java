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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
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
import java.util.concurrent.atomic.AtomicInteger;

@Tags({"Cassandra", "ScyllaDB", "AstraDB", "CQL", "YugabyteDB"})
@CapabilityDescription("Transfer data from FlowFile to CQL engine (support Apache Cassandra, " +
        "ScyllaDB, AstraDB).")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class CqlProcessor extends AbstractProcessor {

    private AtomicInteger counter=new AtomicInteger(0);

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor
            .Builder()
            .name("Batch Size")
            .displayName("Batch Size")
            .description("Size of bulk for data ingest.")
            .required(false)
            .defaultValue("200")
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)   //  StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DRY_RUN = new PropertyDescriptor
            .Builder()
            .name("Dry Run")
            .displayName("Dry Run")
            .description("Dry run for processing (without final write to CQL engine).")
            .required(false)
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success processing")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed processing")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private Setup setup = null;
    private CqlAccess cqlAccess;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = List.of(BATCH_SIZE, DRY_RUN);
        relationships = Set.of(REL_SUCCESS, REL_FAILURE);
//        setup=new Setup();
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
        counter.set(0);
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
        // TODO implement

        boolean dryRun=context.getProperty("Dry Run").asBoolean();

        // define Setup
        Setup newSetup= new Setup();
        newSetup.ipAddresses=new String[]{"10.129.53.159","10.129.53.154","10.129.53.153"};
        newSetup.port=9042;
        newSetup.username="perf";
        // TODO: get password from secure property
        newSetup.setPwd("cGVyZg==");
        newSetup.localDC="datacenter1";
        newSetup.connectionTimeout=900;
        newSetup.requestTimeout=60;
        newSetup.consistencyLevel="LOCAL_ONE";
        newSetup.table="prftest.csv2cql_test3";
        newSetup.setBatch(context.getProperty("Batch Size").asLong());

        //  if setup is different then use new setup and cqlAccess
        //      or cqlAccess will be still the same
        if ((setup == null) || (!setup.equals(newSetup))){
            setup = newSetup;
            cqlAccess = new CqlAccess(setup);
            session.putAttribute(flowFile, "CQLAccess","NEW");
        }
        else session.putAttribute(flowFile, "CQLAccess","REUSE");

        //  get CSV
        String csv = this.getContent(flowFile,session);

        CsvCqlWrite write=new CsvCqlWrite(cqlAccess, dryRun);
        try {
            write.execute(null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
//        try {
//            write.execute(null);
//        } catch (CsvValidationException e) {
//            throw new RuntimeException(e);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
        //  write CSV


//        //  get property
//        context.getProperty("");
//
//        //  read attribute
//        flowFile.getAttribute("");

        //  write attribute
//        counter.addAndGet(1);
//        session.putAttribute(flowFile, "newprop_jirka","value steuer");
//        session.putAttribute(flowFile, "mycounter", counter.toString());

        // Helpers:
        //  https://medium.com/@tomerdayan168/build-your-processors-in-nifi-7bb0f217ed75
        //  https://help.hcl-software.com/commerce/9.1.0/search/tasks/t_createcustomnifi.html

        session.transfer(flowFile, REL_SUCCESS);
    }
}
