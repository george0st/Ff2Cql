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

import java.util.List;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.george0st.cql.helper.ControllerSetup;


// inspiration
// https://github.com/apache/nifi/blob/main/nifi-extension-bundles/nifi-mongodb-bundle/nifi-mongodb-services/src/main/java/org/apache/nifi/mongodb/MongoDBControllerService.java#L187

@Tags({ "cql", "nosql", "cassandra", "scylladb", "cassandra query language", "service"})
@CapabilityDescription("Provides a controller service that configures a connection to CQL solution and " +
        "provides access to that connection to other CQL-related components.")
public class CQLControllerService extends AbstractControllerService implements CQLClientService {

    private String uri;
    protected CQLAccess cqlAccess;

    public static final PropertyDescriptor IP_ADDRESSES = new PropertyDescriptor
            .Builder()
            .name("IP Addresses")
            .description("List of IP addresses for CQL connection, the addresses are split by comma " +
                    "(e.g. '192.168.0.1, 192.168.0.2, ...' or 'localhost').")
            .required(false)
            //.defaultValue("localhost")
            //.addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
            //.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor
            .Builder()
            .name("Port")
            .description("Port for communication.")
            .required(false)
            .defaultValue("9042")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor SECURE_CONNECTION_BUNDLE = new PropertyDescriptor
            .Builder()
            .name("Secure Connection Bundle")
            .description("Secure Connection Bundle for access to AstraDB " +
                    "(it is the link to '*.zip' file, downloaded from AstraDB web). " +
                    "NOTE: the 'username' is 'clientId' and 'password' id 'secret', these values are from " +
                    "the file '*-token.json', downloaded from AstraDB web.")
            .required(false)
            //.addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor
            .Builder()
            .name("Username")
            .description("Username for the CQL connection.")
            .required(false)
            //.addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
            .addValidator(Validator.VALID)
            //.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor
            .Builder()
            .name("Password")
            .description("Password for the CQL connection.")
            .required(false)
            //.addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
            .addValidator(Validator.VALID)
            //.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor LOCAL_DC = new PropertyDescriptor
            .Builder()
            .name("Local Data Center")
            .description("Name of local data center e.g. 'dc1', 'datacenter1', etc.")
            .required(false)
            //.defaultValue("datacenter1")
            //.addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
            .addValidator(Validator.VALID)
            //.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONNECTION_TIMEOUT = new PropertyDescriptor
            .Builder()
            .name("Connection Timeout")
            .description("Timeout for connection to CQL engine (in seconds).")
            .required(true)
            .defaultValue("900")
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();

    public static final PropertyDescriptor REQUEST_TIMEOUT = new PropertyDescriptor
            .Builder()
            .name("Request Timeout")
            .description("Timeout for request to CQL engine (in seconds).")
            .required(true)
            .defaultValue("60")
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONSISTENCY_LEVEL = new PropertyDescriptor
            .Builder()
            .name("Consistency Level")
            .description("Default consistency Level for CQL operations.")
            .required(true)
            .defaultValue(CL_LOCAL_ONE.getValue())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(CL_LOCAL_ONE, CL_LOCAL_QUORUM, CL_LOCAL_SERIAL, CL_EACH_QUORUM, CL_ANY, CL_ONE, CL_TWO, CL_THREE, CL_QUORUM, CL_ALL, CL_SERIAL)
            .build();

    private static final List<PropertyDescriptor> properties = List.of(
            IP_ADDRESSES,
            PORT,
            SECURE_CONNECTION_BUNDLE,
            USERNAME,
            PASSWORD,
            LOCAL_DC,
            CONNECTION_TIMEOUT,
            REQUEST_TIMEOUT,
            CONSISTENCY_LEVEL);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * Closed shared access to CQL
     */
    protected void closeAccess(){
        if (cqlAccess != null) {
            cqlAccess.close();
            cqlAccess = null;
        }
    }

    /**
     * Create shared access to CQL, based on setting in Controller
     * @param context   Controller context
     */
    protected void createAccess(final ConfigurationContext context, boolean test){
        try {
            // close (if open)
            closeAccess();

            //  create access
            cqlAccess = new CQLAccess(new ControllerSetup(context));

            if (test) {
                //  test connection
                try (CqlSession session = getSession()) {
                    String ipAddress=cqlAccess.controllerSetup.getIPAddresses();
                    getLogger().info("SUCCESS connection [{}] !!!",
                            ipAddress!=null ? "IP - " + ipAddress : "SCB - " + cqlAccess.controllerSetup.secureConnectionBundle);
                }
            }
        } catch(Exception ex){
            getLogger().error("createAccess");
            throw ex;
        }
    }

    /**
     * @param context
     *            the configuration context
     * @throws InitializationException
     *             if unable to create a database connection
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        this.uri = getURI(context);

        //  create new access based on controllerSetup (and test connection)
        createAccess(context, true);
    }

    protected String getURI(final ConfigurationContext context) {
        //final String ip = context.getProperty(IP_ADDRESSES).evaluateAttributeExpressions().getValue();
        final String ip = context.getProperty(IP_ADDRESSES).getValue();
        final String secureConnectionBundl = context.getProperty(SECURE_CONNECTION_BUNDLE).getValue();

//        final String port = context.getProperty(PORT).evaluateAttributeExpressions().getValue();
//        final String user = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
//        if (!ip.contains("@") && user != null && passw != null) {
//            return uri.replaceFirst("://", "://" + URLEncoder.encode(user, StandardCharsets.UTF_8) + ":" + URLEncoder.encode(passw, StandardCharsets.UTF_8) + "@");
//        } else {
//            return uri;
//        }
        return ip != null ? ip: secureConnectionBundl;
    }

    @Override
    public String getURI() {
        return uri;
    }

    @OnDisabled
    public void shutdown() {
        closeAccess();
    }

    @Override
    public CqlSession getSession() { return cqlAccess != null ? cqlAccess.sessionBuilder.build() : null; }


}
