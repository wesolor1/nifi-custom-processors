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
package org.apache.nifi.processors.aws.credentials.provider.service;

import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processors.mbbel.ListS3Extended;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class AWSCredentialsProviderControllerServiceExtendedTest {

    @AfterEach
    public void tearDown() {
        AWSCredentialsProviderControllerServiceExtended.clearEvaluationAttributes();
    }

    @Test
    public void testAccessKeyPropertiesAreNotSensitive() {
        assertFalse(AWSCredentialsProviderControllerServiceExtended.ACCESS_KEY_ID.isSensitive(),
                "Access Key ID must be non-sensitive so plain-text or Expression Language values can be configured");
        assertFalse(AWSCredentialsProviderControllerServiceExtended.SECRET_KEY.isSensitive(),
                "Secret Access Key must be non-sensitive so plain-text or Expression Language values can be configured");
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                AWSCredentialsProviderControllerServiceExtended.ACCESS_KEY_ID.getExpressionLanguageScope());
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                AWSCredentialsProviderControllerServiceExtended.SECRET_KEY.getExpressionLanguageScope());
    }

    @Test
    public void testAccessKeyExpressionLanguageResolvedFromAttributes() throws InitializationException {
        final AWSCredentialsProviderControllerServiceExtended credentialsService = new AWSCredentialsProviderControllerServiceExtended();
        final TestRunner runner = TestRunners.newTestRunner(ListS3Extended.class);

        runner.addControllerService("aws-creds", credentialsService);
        runner.setProperty(credentialsService, AWSCredentialsProviderControllerServiceExtended.ACCESS_KEY_ID, "${source_access_key_id}");
        runner.setProperty(credentialsService, AWSCredentialsProviderControllerServiceExtended.SECRET_KEY, "${source_secret_access_key}");
        runner.enableControllerService(credentialsService);

        AWSCredentialsProviderControllerServiceExtended.setEvaluationAttributes(Map.of(
                "source_access_key_id", "resolved-access-key",
                "source_secret_access_key", "resolved-secret-key"
        ));

        final AwsCredentialsProvider provider = credentialsService.getAwsCredentialsProvider();
        final AwsBasicCredentials credentials = (AwsBasicCredentials) provider.resolveCredentials();

        assertEquals("resolved-access-key", credentials.accessKeyId());
        assertEquals("resolved-secret-key", credentials.secretAccessKey());
    }
}
