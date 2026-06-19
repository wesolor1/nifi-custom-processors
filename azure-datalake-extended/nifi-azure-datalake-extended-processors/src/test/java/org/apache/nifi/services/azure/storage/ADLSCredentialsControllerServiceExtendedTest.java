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
package org.apache.nifi.services.azure.storage;

import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processors.azure.AzureServiceEndpoints;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.processors.mbbel.ListAzureDataLakeStorageExtended;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class ADLSCredentialsControllerServiceExtendedTest {

    @Test
    public void testStorageAccountNameIsNotSensitive() {
        assertFalse(ADLSCredentialsControllerServiceExtended.ACCOUNT_NAME.isSensitive(),
                "Storage Account Name must be non-sensitive so FlowFile-attribute Expression Language can be configured");
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                ADLSCredentialsControllerServiceExtended.ACCOUNT_NAME.getExpressionLanguageScope());
    }

    @Test
    public void testStorageAccountNameExpressionLanguageResolvedFromAttributes() throws InitializationException {
        final ADLSCredentialsControllerServiceExtended credentialsService = new ADLSCredentialsControllerServiceExtended();
        final TestRunner runner = TestRunners.newTestRunner(ListAzureDataLakeStorageExtended.class);

        runner.addControllerService("adls-creds", credentialsService);
        runner.setProperty(credentialsService, ADLSCredentialsControllerServiceExtended.ACCOUNT_NAME, "${source_storage_account}");
        runner.setProperty(credentialsService, ADLSCredentialsControllerServiceExtended.ENDPOINT_SUFFIX,
                AzureServiceEndpoints.DEFAULT_ADLS_ENDPOINT_SUFFIX);
        runner.setProperty(credentialsService, AzureStorageUtils.CREDENTIALS_TYPE,
                AzureStorageCredentialsType.MANAGED_IDENTITY.getValue());
        runner.enableControllerService(credentialsService);

        final ADLSCredentialsDetails details = credentialsService.getCredentialsDetails(
                Map.of("source_storage_account", "resolved-storage-account"));

        assertEquals("resolved-storage-account", details.getAccountName());
    }
}
