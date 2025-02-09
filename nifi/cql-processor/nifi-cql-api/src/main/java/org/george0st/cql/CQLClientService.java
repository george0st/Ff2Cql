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

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.controller.ControllerService;

@Tags({"CQL", "service", "interface"})
@CapabilityDescription("Interface for controller service that configures a connection to CQL solution.")
public interface CQLClientService extends ControllerService {

    AllowableValue CL_LOCAL_ONE = new AllowableValue("LOCAL_ONE", "LOCAL_ONE");
    AllowableValue CL_LOCAL_QUORUM = new AllowableValue("LOCAL_QUORUM", "LOCAL_QUORUM");
    AllowableValue CL_LOCAL_SERIAL = new AllowableValue("LOCAL_SERIAL", "LOCAL_SERIAL");
    AllowableValue CL_EACH_QUORUM = new AllowableValue("EACH_QUORUM", "EACH_QUORUM");
    AllowableValue CL_ANY = new AllowableValue("ANY", "ANY");
    AllowableValue CL_ONE = new AllowableValue("ONE", "ONE");
    AllowableValue CL_TWO = new AllowableValue("TWO", "TWO");
    AllowableValue CL_THREE = new AllowableValue("THREE", "THREE");
    AllowableValue CL_QUORUM = new AllowableValue("QUORUM", "QUORUM");
    AllowableValue CL_ALL = new AllowableValue("ALL", "ALL");
    AllowableValue CL_SERIAL = new AllowableValue("SERIAL", "SERIAL");

    /**
     * The uri for transitUri in provenance reporter
     * @return  text uri
     */
    String getURI();

    /**
     * Get session for access to CQL solution
     * @return  new cql session
     */
    CqlSession getSession();

}
