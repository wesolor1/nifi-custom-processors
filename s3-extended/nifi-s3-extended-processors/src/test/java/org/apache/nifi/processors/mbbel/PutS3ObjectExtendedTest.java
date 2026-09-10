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
package org.apache.nifi.processors.mbbel;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.aws.AbstractAwsProcessor;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerServiceExtended;
import org.apache.nifi.processors.aws.region.RegionUtil;
import org.apache.nifi.processors.aws.s3.AbstractS3Processor;
import org.apache.nifi.processors.aws.s3.PutS3Object;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PutS3ObjectExtendedTest {

    /**
     * Test double that records credentials resolved through the same {@code getClient}/{@code getCredentialsProvider}
     * hooks stock PutS3Object uses, without contacting S3.
     */
    public static class TestablePutS3ObjectExtended extends PutS3ObjectExtended {
        volatile AwsCredentials resolvedCredentials;

        @Override
        public void onScheduled(final ProcessContext context) {
            // Skip AbstractAwsProcessor's eager S3 client creation; this test double never contacts S3.
        }

        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) {
            final FlowFile flowFile = session.get();
            if (flowFile == null) {
                return;
            }
            try {
                captureTriggerAttributes(flowFile.getAttributes());
                resolvedCredentials = getCredentialsProvider(context).resolveCredentials();
                session.transfer(flowFile, REL_SUCCESS);
            } finally {
                // Mirror the production finally so cleanup can be asserted on this test double as well.
                AWSCredentialsProviderControllerServiceExtended.clearEvaluationAttributes();
            }
        }
    }

    @AfterEach
    public void tearDown() {
        AWSCredentialsProviderControllerServiceExtended.clearEvaluationAttributes();
    }

    private TestRunner newRunner(final PutS3ObjectExtended processor) throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(processor);
        final AWSCredentialsProviderControllerServiceExtended credentials =
                new AWSCredentialsProviderControllerServiceExtended();
        runner.addControllerService("aws-creds", credentials);
        runner.setProperty(credentials, AWSCredentialsProviderControllerServiceExtended.ACCESS_KEY_ID,
                "${source_access_key_id}");
        runner.setProperty(credentials, AWSCredentialsProviderControllerServiceExtended.SECRET_KEY,
                "${source_secret_access_key}");
        runner.enableControllerService(credentials);
        runner.setProperty(AbstractAwsProcessor.AWS_CREDENTIALS_PROVIDER_SERVICE, "aws-creds");
        runner.setProperty(RegionUtil.REGION, "us-east-1");
        runner.setProperty(AbstractS3Processor.BUCKET_WITH_DEFAULT_VALUE, "my-bucket");
        return runner;
    }

    @Test
    public void testInputRequired() {
        final InputRequirement requirement = PutS3ObjectExtended.class.getAnnotation(InputRequirement.class);
        assertNotNull(requirement, "@InputRequirement should be present on PutS3ObjectExtended");
        assertEquals(Requirement.INPUT_REQUIRED, requirement.value());
    }

    @Test
    public void testPropertiesMatchStockPutS3Object() {
        final List<PropertyDescriptor> base = new PutS3Object().getPropertyDescriptors();
        final List<PropertyDescriptor> extended = new PutS3ObjectExtended().getPropertyDescriptors();

        assertEquals(base.size(), extended.size(), "Extended processor must expose the same set of properties");
        for (int i = 0; i < base.size(); i++) {
            assertEquals(base.get(i), extended.get(i),
                    "Property ordering and values must be unchanged: " + base.get(i).getName());
        }
    }

    @Test
    public void testRelationshipsAreInheritedFromPutS3Object() {
        final PutS3ObjectExtended extended = new PutS3ObjectExtended();
        for (final Relationship relationship : new PutS3Object().getRelationships()) {
            assertTrue(extended.getRelationships().stream().anyMatch(r -> r.getName().equals(relationship.getName())),
                    "Extended processor should expose stock PutS3Object relationship: " + relationship.getName());
        }
    }

    @Test
    public void testSeeAlsoIncludesExtendedCredentialsServiceAndListS3Extended() {
        final SeeAlso seeAlso = PutS3ObjectExtended.class.getAnnotation(SeeAlso.class);
        assertNotNull(seeAlso);
        assertTrue(Arrays.asList(seeAlso.value()).contains(AWSCredentialsProviderControllerServiceExtended.class));
        assertTrue(Arrays.asList(seeAlso.value()).contains(ListS3Extended.class));
        assertTrue(Arrays.asList(seeAlso.value()).contains(PutS3Object.class));
        assertTrue(Arrays.asList(seeAlso.value()).contains(FetchS3ObjectExtended.class));
        assertTrue(Arrays.asList(seeAlso.value()).contains(DeleteS3ObjectExtended.class));
    }

    @Test
    public void testIncomingAttributesResolveCredentialsExpressionLanguage() throws InitializationException {
        final TestablePutS3ObjectExtended processor = new TestablePutS3ObjectExtended();
        final TestRunner runner = newRunner(processor);

        runner.enqueue("payload".getBytes(), Map.of(
                "source_access_key_id", "resolved-access-key",
                "source_secret_access_key", "resolved-secret-key",
                "filename", "object.txt"
        ));
        runner.run();

        runner.assertTransferCount(PutS3ObjectExtended.REL_SUCCESS, 1);
        assertNotNull(processor.resolvedCredentials, "Credentials should have been resolved against FlowFile attributes");
        final AwsBasicCredentials credentials = (AwsBasicCredentials) processor.resolvedCredentials;
        assertEquals("resolved-access-key", credentials.accessKeyId());
        assertEquals("resolved-secret-key", credentials.secretAccessKey());
    }

    @Test
    public void testEvaluationAttributesClearedAfterTrigger() throws Exception {
        final TestablePutS3ObjectExtended processor = new TestablePutS3ObjectExtended();
        final TestRunner runner = newRunner(processor);

        runner.enqueue("payload".getBytes(), Map.of(
                "source_access_key_id", "resolved-access-key",
                "source_secret_access_key", "resolved-secret-key",
                "filename", "object.txt"
        ));
        runner.run();

        final Field field = AWSCredentialsProviderControllerServiceExtended.class.getDeclaredField("EVALUATION_ATTRIBUTES");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        final ThreadLocal<Map<String, String>> threadLocal = (ThreadLocal<Map<String, String>>) field.get(null);
        assertNull(threadLocal.get(),
                "Evaluation attributes must be cleared after onTrigger to avoid leaking onto other components");
    }
}
