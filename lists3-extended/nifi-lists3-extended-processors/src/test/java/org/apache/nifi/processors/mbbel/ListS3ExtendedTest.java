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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.aws.AbstractAwsProcessor;
import org.apache.nifi.processors.aws.credentials.provider.AwsCredentialsProviderService;
import org.apache.nifi.processors.aws.region.RegionUtil;
import org.apache.nifi.processors.aws.s3.AbstractS3Processor;
import org.apache.nifi.processors.aws.s3.ListS3;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ListS3ExtendedTest {

    private static final String CREDENTIALS_SERVICE_ID = "creds";

    /**
     * Minimal stub so the processor validates inside a TestRunner without requiring real AWS credentials.
     */
    public static class StubCredentialsService extends AbstractControllerService implements AwsCredentialsProviderService {
        @Override
        public AwsCredentialsProvider getAwsCredentialsProvider() {
            return AnonymousCredentialsProvider.create();
        }
    }

    /**
     * Test double that records what context/session the listing was invoked with and emits a single listing FlowFile
     * (mimicking the stock AttributeObjectWriter), without contacting S3. The bucket value is resolved through the
     * supplied context so we can assert Expression Language is evaluated against the trigger FlowFile attributes.
     */
    public static class TestableListS3Extended extends ListS3Extended {
        volatile boolean listingPerformed = false;
        volatile String resolvedBucket;
        volatile boolean emitFlowFile = true;

        @Override
        public void onScheduled(final ProcessContext context) {
            // Skip AbstractAwsProcessor's eager S3 client creation; this test double never contacts S3.
        }

        @Override
        protected void performListing(final ProcessContext context, final ProcessSession session) {
            listingPerformed = true;
            // The context (FlowFileAwareProcessContext when triggered) already resolves Bucket EL against the
            // trigger attributes, so read the resolved value directly rather than re-evaluating.
            resolvedBucket = context.getProperty(AbstractS3Processor.BUCKET_WITHOUT_DEFAULT_VALUE).getValue();

            if (!emitFlowFile) {
                return;
            }

            FlowFile listed = session.create();
            final Map<String, String> attributes = new HashMap<>();
            attributes.put("filename", "object-key.txt");
            attributes.put("s3.bucket", resolvedBucket);
            // Intentionally also set an attribute that may collide with a trigger attribute to prove precedence.
            attributes.put("origin", "listing");
            listed = session.putAllAttributes(listed, attributes);
            session.transfer(listed, REL_SUCCESS);
        }
    }

    private TestRunner newRunner(final ListS3Extended processor) throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(processor);
        final StubCredentialsService credentials = new StubCredentialsService();
        runner.addControllerService(CREDENTIALS_SERVICE_ID, credentials);
        runner.enableControllerService(credentials);
        runner.setProperty(AbstractAwsProcessor.AWS_CREDENTIALS_PROVIDER_SERVICE, CREDENTIALS_SERVICE_ID);
        runner.setProperty(RegionUtil.REGION, "us-east-1");
        return runner;
    }

    private PropertyDescriptor exposedDescriptor(final ListS3Extended processor, final String name) {
        return processor.getPropertyDescriptors().stream()
                .filter(pd -> pd.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Descriptor not exposed by processor: " + name));
    }

    @Test
    public void testIncomingConnectionsAreAllowed() {
        final InputRequirement requirement = ListS3Extended.class.getAnnotation(InputRequirement.class);
        assertNotNull(requirement, "@InputRequirement should be present on ListS3Extended");
        assertEquals(Requirement.INPUT_ALLOWED, requirement.value(),
                "ListS3Extended must allow incoming connections (stock ListS3 forbids them)");
    }

    @Test
    public void testBucketAndPrefixSupportFlowFileAttributesExpressionLanguage() {
        final ListS3Extended processor = new ListS3Extended();
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                exposedDescriptor(processor, ListS3Extended.BUCKET.getName()).getExpressionLanguageScope(),
                "Bucket must be evaluatable against FlowFile attributes");
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                exposedDescriptor(processor, ListS3Extended.PREFIX.getName()).getExpressionLanguageScope(),
                "Prefix must be evaluatable against FlowFile attributes");
    }

    @Test
    public void testOnlyBucketAndPrefixDifferFromStockListS3() {
        final List<PropertyDescriptor> base = new ListS3().getPropertyDescriptors();
        final List<PropertyDescriptor> extended = new ListS3Extended().getPropertyDescriptors();

        assertEquals(base.size(), extended.size(), "Extended processor must expose the same set of properties");
        for (int i = 0; i < base.size(); i++) {
            final PropertyDescriptor baseDescriptor = base.get(i);
            final PropertyDescriptor extendedDescriptor = extended.get(i);
            assertEquals(baseDescriptor.getName(), extendedDescriptor.getName(),
                    "Property ordering and names must be preserved");
            if (baseDescriptor.getName().equals(ListS3Extended.BUCKET.getName())
                    || baseDescriptor.getName().equals(ListS3Extended.PREFIX.getName())) {
                assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES, extendedDescriptor.getExpressionLanguageScope(),
                        "Overridden property must support FlowFile attribute EL: " + baseDescriptor.getName());
            } else {
                assertEquals(baseDescriptor.getExpressionLanguageScope(), extendedDescriptor.getExpressionLanguageScope(),
                        "EL scope of other properties must be unchanged: " + baseDescriptor.getName());
            }
        }
    }

    @Test
    public void testRelationshipsAreInheritedFromListS3() {
        final ListS3Extended extended = new ListS3Extended();
        for (final Relationship relationship : new ListS3().getRelationships()) {
            assertTrue(extended.getRelationships().stream().anyMatch(r -> r.getName().equals(relationship.getName())),
                    "Extended processor should expose stock ListS3 relationship: " + relationship.getName());
        }
    }

    @Test
    public void testTriggerFlowFileIsConsumedAndAttributesCopiedToListing() throws InitializationException {
        final TestableListS3Extended processor = new TestableListS3Extended();
        final TestRunner runner = newRunner(processor);
        runner.setProperty(ListS3Extended.BUCKET, "my-bucket");

        final Map<String, String> triggerAttributes = new HashMap<>();
        triggerAttributes.put("batch.id", "12345");
        triggerAttributes.put("origin", "trigger");
        runner.enqueue("trigger".getBytes(), triggerAttributes);

        runner.run();

        assertTrue(processor.listingPerformed, "Listing should have been performed on the incoming trigger");
        runner.assertQueueEmpty();
        runner.assertTransferCount(ListS3.REL_SUCCESS, 1);

        final MockFlowFile out = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS).get(0);
        out.assertAttributeEquals("filename", "object-key.txt");
        out.assertAttributeEquals("batch.id", "12345");
        // Listing attribute wins over the trigger attribute on name collision.
        out.assertAttributeEquals("origin", "listing");
    }

    @Test
    public void testBucketExpressionLanguageResolvedFromTriggerAttributes() throws InitializationException {
        final TestableListS3Extended processor = new TestableListS3Extended();
        final TestRunner runner = newRunner(processor);
        runner.setProperty(ListS3Extended.BUCKET, "${target.bucket}");

        final Map<String, String> triggerAttributes = new HashMap<>();
        triggerAttributes.put("target.bucket", "resolved-bucket");
        runner.enqueue("trigger".getBytes(), triggerAttributes);

        runner.run();

        assertEquals("resolved-bucket", processor.resolvedBucket,
                "Bucket Expression Language should resolve against the trigger FlowFile attributes");
        final MockFlowFile out = runner.getFlowFilesForRelationship(ListS3.REL_SUCCESS).get(0);
        out.assertAttributeEquals("s3.bucket", "resolved-bucket");
    }

    @Test
    public void testEmptyIncomingConnectionYieldsWithoutListing() throws InitializationException {
        final TestableListS3Extended processor = new TestableListS3Extended();
        final TestRunner runner = newRunner(processor);
        runner.setProperty(ListS3Extended.BUCKET, "my-bucket");
        runner.setIncomingConnection(true);

        runner.run();

        assertFalse(processor.listingPerformed,
                "With an empty incoming connection the processor should yield instead of listing");
        runner.assertTransferCount(ListS3.REL_SUCCESS, 0);
    }
}
