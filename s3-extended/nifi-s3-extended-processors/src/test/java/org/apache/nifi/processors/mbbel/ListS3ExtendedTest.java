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
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
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
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
        volatile String resolvedEndpoint;
        volatile boolean emitFlowFile = true;
        volatile boolean bucketHasObjects = true;
        volatile RuntimeException preflightException;
        volatile List<String> listedKeys = List.of("object-key.txt");

        @Override
        public void onScheduled(final ProcessContext context) {
            // Skip AbstractAwsProcessor's eager S3 client creation; this test double never contacts S3.
        }

        @Override
        protected boolean remoteHasObjects(final ProcessContext context) {
            if (preflightException != null) {
                throw preflightException;
            }
            return bucketHasObjects;
        }

        @Override
        protected void performListing(final ProcessContext context, final ProcessSession session) {
            listingPerformed = true;
            // The context (FlowFileAwareProcessContext when triggered) already resolves Bucket EL against the
            // trigger attributes, so read the resolved value directly rather than re-evaluating.
            resolvedBucket = context.getProperty(AbstractS3Processor.BUCKET_WITHOUT_DEFAULT_VALUE).getValue();
            resolvedEndpoint = context.getProperty(ListS3Extended.ENDPOINT_OVERRIDE).getValue();

            if (!emitFlowFile) {
                return;
            }

            for (final String key : listedKeys) {
                FlowFile listed = session.create();
                final Map<String, String> attributes = new HashMap<>();
                attributes.put("filename", key);
                attributes.put("s3.bucket", resolvedBucket);
                // Intentionally also set an attribute that may collide with a trigger attribute to prove precedence.
                attributes.put("origin", "listing");
                listed = session.putAllAttributes(listed, attributes);
                session.transfer(listed, REL_SUCCESS);
            }
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
    public void testPrimaryNodeOnlyIsNotInherited() {
        // @PrimaryNodeOnly is @Inherited and NiFi walks superclasses, which is why this processor
        // extends AbstractS3Processor directly instead of subclassing the stock ListS3.
        assertNull(ListS3Extended.class.getAnnotation(PrimaryNodeOnly.class),
                "ListS3Extended must not be primary-node-only so incoming connections and All Nodes execution are allowed");
        assertFalse(ListS3.class.isAssignableFrom(ListS3Extended.class),
                "ListS3Extended must not extend ListS3 or @PrimaryNodeOnly would still be discovered");
    }

    @Test
    public void testBucketPrefixAndEndpointSupportFlowFileAttributesExpressionLanguage() {
        final ListS3Extended processor = new ListS3Extended();
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                exposedDescriptor(processor, ListS3Extended.BUCKET.getName()).getExpressionLanguageScope(),
                "Bucket must be evaluatable against FlowFile attributes");
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                exposedDescriptor(processor, ListS3Extended.PREFIX.getName()).getExpressionLanguageScope(),
                "Prefix must be evaluatable against FlowFile attributes");
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                exposedDescriptor(processor, ListS3Extended.ENDPOINT_OVERRIDE.getName()).getExpressionLanguageScope(),
                "Endpoint Override URL must be evaluatable against FlowFile attributes");
    }

    @Test
    public void testStockPropertiesPreservedAndFilePathFiltersAdded() {
        final List<PropertyDescriptor> base = new ListS3().getPropertyDescriptors();
        final List<PropertyDescriptor> extended = new ListS3Extended().getPropertyDescriptors();

        assertEquals(base.size() + 2, extended.size(),
                "Extended processor must expose stock ListS3 properties plus File Filter and Path Filter");

        final List<String> baseNames = base.stream().map(PropertyDescriptor::getName).toList();
        final List<String> extendedNames = extended.stream().map(PropertyDescriptor::getName).toList();
        assertTrue(extendedNames.containsAll(baseNames), "Stock ListS3 properties must still be present");
        assertTrue(extendedNames.contains(ListS3Extended.FILE_FILTER.getName()));
        assertTrue(extendedNames.contains(ListS3Extended.PATH_FILTER.getName()));

        for (final PropertyDescriptor extendedDescriptor : extended) {
            if (extendedDescriptor.getName().equals(ListS3Extended.BUCKET.getName())
                    || extendedDescriptor.getName().equals(ListS3Extended.PREFIX.getName())
                    || extendedDescriptor.getName().equals(ListS3Extended.ENDPOINT_OVERRIDE.getName())
                    || extendedDescriptor.getName().equals(ListS3Extended.FILE_FILTER.getName())
                    || extendedDescriptor.getName().equals(ListS3Extended.PATH_FILTER.getName())) {
                assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES, extendedDescriptor.getExpressionLanguageScope(),
                        "Overridden/added property must support FlowFile attribute EL: " + extendedDescriptor.getName());
            } else {
                final PropertyDescriptor baseDescriptor = base.stream()
                        .filter(pd -> pd.getName().equals(extendedDescriptor.getName()))
                        .findFirst()
                        .orElseThrow();
                assertEquals(baseDescriptor.getExpressionLanguageScope(), extendedDescriptor.getExpressionLanguageScope(),
                        "EL scope of other properties must be unchanged: " + extendedDescriptor.getName());
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
        runner.assertTransferCount(ListS3Extended.REL_SUCCESS, 1);

        final MockFlowFile out = runner.getFlowFilesForRelationship(ListS3Extended.REL_SUCCESS).get(0);
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
        final MockFlowFile out = runner.getFlowFilesForRelationship(ListS3Extended.REL_SUCCESS).get(0);
        out.assertAttributeEquals("s3.bucket", "resolved-bucket");
    }

    @Test
    public void testEndpointOverrideExpressionLanguageResolvedFromTriggerAttributes() throws InitializationException {
        final TestableListS3Extended processor = new TestableListS3Extended();
        final TestRunner runner = newRunner(processor);
        runner.setProperty(ListS3Extended.BUCKET, "my-bucket");
        runner.setProperty(ListS3Extended.ENDPOINT_OVERRIDE, "${source_endpoint_override}");

        final Map<String, String> triggerAttributes = new HashMap<>();
        triggerAttributes.put("source_endpoint_override", "https://s3-fracldhpc.app.corpintra.net:443");
        runner.enqueue("trigger".getBytes(), triggerAttributes);

        runner.run();

        assertEquals("https://s3-fracldhpc.app.corpintra.net:443", processor.resolvedEndpoint,
                "Endpoint Override Expression Language should resolve against the trigger FlowFile attributes");
    }

    @Test
    public void testRelationshipsIncludeFailureAndNoFiles() {
        final ListS3Extended extended = new ListS3Extended();
        final List<String> names = extended.getRelationships().stream().map(Relationship::getName).toList();
        assertTrue(names.contains("success"), "Extended processor should expose the success relationship");
        assertTrue(names.contains("Failure"), "Extended processor should expose the Failure relationship");
        assertTrue(names.contains("No Files"), "Extended processor should expose the No Files relationship");
        assertTrue(extended.getRelationships().contains(ListS3Extended.REL_FAILURE));
        assertTrue(extended.getRelationships().contains(ListS3Extended.REL_NO_FILES));
    }

    @Test
    public void testListingFailureRoutesErrorFlowFileWithTriggerAttributes() throws InitializationException {
        final TestableListS3Extended processor = new TestableListS3Extended();
        processor.preflightException = new RuntimeException("simulated S3 failure");
        final TestRunner runner = newRunner(processor);
        runner.setProperty(ListS3Extended.BUCKET, "my-bucket");

        final Map<String, String> triggerAttributes = new HashMap<>();
        triggerAttributes.put("batch.id", "12345");
        runner.enqueue("trigger".getBytes(), triggerAttributes);

        runner.run();

        runner.assertQueueEmpty();
        runner.assertTransferCount(ListS3Extended.REL_SUCCESS, 0);
        runner.assertTransferCount(ListS3Extended.REL_FAILURE, 1);

        final MockFlowFile out = runner.getFlowFilesForRelationship(ListS3Extended.REL_FAILURE).get(0);
        out.assertAttributeEquals("error.message", "simulated S3 failure");
        out.assertAttributeEquals("error.class", RuntimeException.class.getName());
        out.assertAttributeEquals("s3.bucket", "my-bucket");
        out.assertAttributeEquals("batch.id", "12345");
    }

    @Test
    public void testEmptyBucketRoutesZeroRecordFlowFileToNoFiles() throws InitializationException {
        final TestableListS3Extended processor = new TestableListS3Extended();
        processor.bucketHasObjects = false;
        final TestRunner runner = newRunner(processor);
        runner.setProperty(ListS3Extended.BUCKET, "my-bucket");

        final MockRecordWriter writerFactory = new MockRecordWriter(null, false);
        runner.addControllerService("record-writer", writerFactory);
        runner.enableControllerService(writerFactory);
        runner.setProperty(ListS3.RECORD_WRITER, "record-writer");

        final Map<String, String> triggerAttributes = new HashMap<>();
        triggerAttributes.put("batch.id", "12345");
        runner.enqueue("trigger".getBytes(), triggerAttributes);

        runner.run();

        assertFalse(processor.listingPerformed, "The stock listing should be skipped when the bucket is empty");
        runner.assertQueueEmpty();
        runner.assertTransferCount(ListS3Extended.REL_SUCCESS, 0);
        runner.assertTransferCount(ListS3Extended.REL_FAILURE, 0);
        runner.assertTransferCount(ListS3Extended.REL_NO_FILES, 1);

        final MockFlowFile out = runner.getFlowFilesForRelationship(ListS3Extended.REL_NO_FILES).get(0);
        out.assertAttributeEquals("record.count", "0");
        out.assertAttributeEquals("s3.bucket", "my-bucket");
        out.assertAttributeEquals("batch.id", "12345");
    }

    @Test
    public void testNoFilesIsNotEmittedWithoutRecordWriter() throws InitializationException {
        final TestableListS3Extended processor = new TestableListS3Extended();
        processor.bucketHasObjects = false;
        final TestRunner runner = newRunner(processor);
        runner.setProperty(ListS3Extended.BUCKET, "my-bucket");

        runner.enqueue("trigger".getBytes());
        runner.run();

        runner.assertTransferCount(ListS3Extended.REL_NO_FILES, 0);
        runner.assertTransferCount(ListS3Extended.REL_FAILURE, 0);
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
        runner.assertTransferCount(ListS3Extended.REL_SUCCESS, 0);
    }

    @Test
    public void testSeeAlsoIncludesExtendedFetchPutAndDelete() {
        final SeeAlso seeAlso = ListS3Extended.class.getAnnotation(SeeAlso.class);
        assertNotNull(seeAlso);
        assertTrue(Arrays.asList(seeAlso.value()).contains(FetchS3ObjectExtended.class));
        assertTrue(Arrays.asList(seeAlso.value()).contains(DeleteS3ObjectExtended.class));
        assertTrue(Arrays.asList(seeAlso.value()).contains(PutS3ObjectExtended.class));
    }

    @Test
    public void testFileAndPathFilterSupportFlowFileAttributesExpressionLanguage() {
        final ListS3Extended processor = new ListS3Extended();
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                exposedDescriptor(processor, ListS3Extended.FILE_FILTER.getName()).getExpressionLanguageScope());
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                exposedDescriptor(processor, ListS3Extended.PATH_FILTER.getName()).getExpressionLanguageScope());
    }

    @Test
    public void testFileAndPathFilterAcceptExpressionLanguageWithoutValidationError() throws InitializationException {
        final TestRunner runner = newRunner(new ListS3Extended());
        runner.setProperty(ListS3Extended.BUCKET, "my-bucket");
        runner.setProperty(ListS3Extended.FILE_FILTER, "${file_filter}");
        runner.setProperty(ListS3Extended.PATH_FILTER, "${path_filter}");
        runner.assertValid();
    }

    @Test
    public void testInvalidLiteralRegexFilterIsRejected() throws InitializationException {
        final TestRunner runner = newRunner(new ListS3Extended());
        runner.setProperty(ListS3Extended.BUCKET, "my-bucket");
        runner.setProperty(ListS3Extended.FILE_FILTER, "[");
        runner.assertNotValid();
    }

    @Test
    public void testBlankResolvedFilterIsIgnored() throws InitializationException {
        final TestRunner runner = newRunner(new ListS3Extended());
        runner.setProperty(ListS3Extended.BUCKET, "my-bucket");
        runner.setProperty(ListS3Extended.FILE_FILTER, "${file_filter}");
        final PropertyValue fileFilter = runner.getProcessContext().getProperty(ListS3Extended.FILE_FILTER);

        assertNull(ListS3Extended.compileFilterOrNull(fileFilter, Map.of()));
        assertNull(ListS3Extended.compileFilterOrNull(fileFilter, Map.of("file_filter", "")));
        assertNotNull(ListS3Extended.compileFilterOrNull(fileFilter, Map.of("file_filter", "te.*")));
    }

    @Test
    public void testFilenameAndPathOfS3Key() {
        assertEquals("file.xml", ListS3Extended.filenameOfKey("NIFIDEV/DATA/GOODWILL/Output/discount/file.xml"));
        assertEquals("NIFIDEV/DATA/GOODWILL/Output/discount",
                ListS3Extended.pathOfKey("NIFIDEV/DATA/GOODWILL/Output/discount/file.xml"));
        assertEquals("root.xml", ListS3Extended.filenameOfKey("root.xml"));
        assertEquals("", ListS3Extended.pathOfKey("root.xml"));
    }

    @Test
    public void testFileFilterKeepsMatchingObjectNames() throws InitializationException {
        final TestableListS3Extended processor = new TestableListS3Extended();
        processor.listedKeys = List.of(
                "NIFIDEV/DATA/GOODWILL/Output/discount/file.xml",
                "NIFIDEV/DATA/GOODWILL/Output/discount/file.txt",
                "NIFIDEV/DATA/GOODWILL/Output/discount/README.XML");
        final TestRunner runner = newRunner(processor);
        runner.setProperty(ListS3Extended.BUCKET, "my-bucket");
        runner.setProperty(ListS3Extended.FILE_FILTER, "(?i)^.*\\.xml$");

        runner.enqueue("trigger".getBytes(), Map.of("batch.id", "B-xml"));
        runner.run();

        runner.assertTransferCount(ListS3Extended.REL_SUCCESS, 2);
        runner.assertTransferCount(ListS3Extended.REL_NO_FILES, 0);
        final List<String> filenames = runner.getFlowFilesForRelationship(ListS3Extended.REL_SUCCESS).stream()
                .map(ff -> ff.getAttribute("filename"))
                .toList();
        assertTrue(filenames.contains("NIFIDEV/DATA/GOODWILL/Output/discount/file.xml"));
        assertTrue(filenames.contains("NIFIDEV/DATA/GOODWILL/Output/discount/README.XML"));
        runner.getFlowFilesForRelationship(ListS3Extended.REL_SUCCESS).get(0).assertAttributeEquals("batch.id", "B-xml");
    }

    @Test
    public void testPathFilterKeepsMatchingParentPaths() throws InitializationException {
        final TestableListS3Extended processor = new TestableListS3Extended();
        processor.listedKeys = List.of(
                "NIFIDEV/DATA/GOODWILL/Output/discount/a.xml",
                "NIFIDEV/DATA/other/b.xml");
        final TestRunner runner = newRunner(processor);
        runner.setProperty(ListS3Extended.BUCKET, "my-bucket");
        runner.setProperty(ListS3Extended.PATH_FILTER, "^NIFIDEV/DATA/GOODWILL/Output/discount$");

        runner.enqueue("trigger".getBytes());
        runner.run();

        runner.assertTransferCount(ListS3Extended.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(ListS3Extended.REL_SUCCESS).get(0)
                .assertAttributeEquals("filename", "NIFIDEV/DATA/GOODWILL/Output/discount/a.xml");
    }

    @Test
    public void testFileFilterExpressionLanguageResolvedFromTriggerAttributes() throws InitializationException {
        final TestableListS3Extended processor = new TestableListS3Extended();
        processor.listedKeys = List.of(
                "NIFIDEV/DATA/file.xml",
                "NIFIDEV/DATA/file.txt");
        final TestRunner runner = newRunner(processor);
        runner.setProperty(ListS3Extended.BUCKET, "my-bucket");
        runner.setProperty(ListS3Extended.FILE_FILTER, "${file_filter}");

        runner.enqueue("trigger".getBytes(), Map.of("file_filter", "(?i)^.*\\.xml$"));
        runner.run();

        runner.assertTransferCount(ListS3Extended.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(ListS3Extended.REL_SUCCESS).get(0)
                .assertAttributeEquals("filename", "NIFIDEV/DATA/file.xml");
    }

    @Test
    public void testBlankResolvedFileFilterListsAllObjects() throws InitializationException {
        final TestableListS3Extended processor = new TestableListS3Extended();
        processor.listedKeys = List.of("a.xml", "b.txt");
        final TestRunner runner = newRunner(processor);
        runner.setProperty(ListS3Extended.BUCKET, "my-bucket");
        runner.setProperty(ListS3Extended.FILE_FILTER, "${file_filter}");

        runner.enqueue("trigger".getBytes());
        runner.run();

        runner.assertTransferCount(ListS3Extended.REL_SUCCESS, 2);
    }

    @Test
    public void testNoFilesRouteWhenAllEntriesFilteredOut() throws InitializationException {
        final TestableListS3Extended processor = new TestableListS3Extended();
        processor.listedKeys = List.of("NIFIDEV/DATA/file.txt", "NIFIDEV/DATA/file.csv");
        final TestRunner runner = newRunner(processor);
        runner.setProperty(ListS3Extended.BUCKET, "my-bucket");
        runner.setProperty(ListS3Extended.FILE_FILTER, ".*\\.xml");

        final MockRecordWriter writerFactory = new MockRecordWriter(null, false);
        runner.addControllerService("record-writer", writerFactory);
        runner.enableControllerService(writerFactory);
        runner.setProperty(ListS3.RECORD_WRITER, "record-writer");

        runner.enqueue("trigger".getBytes(), Map.of("batch.id", "B-filtered"));
        runner.run();

        runner.assertTransferCount(ListS3Extended.REL_NO_FILES, 1);
        runner.assertTransferCount(ListS3Extended.REL_SUCCESS, 0);
        runner.assertTransferCount(ListS3Extended.REL_FAILURE, 0);

        final MockFlowFile emitted = runner.getFlowFilesForRelationship(ListS3Extended.REL_NO_FILES).get(0);
        emitted.assertAttributeEquals("batch.id", "B-filtered");
        emitted.assertAttributeEquals("record.count", "0");
    }

    @Test
    public void testMatchesFiltersUsesFilenameNotFullKey() {
        final Pattern xmlName = Pattern.compile("(?i)^.*\\.xml$");
        final Pattern discountPath = Pattern.compile("^NIFIDEV/DATA/GOODWILL/Output/discount$");

        assertTrue(ListS3Extended.matchesFilters(
                "NIFIDEV/DATA/GOODWILL/Output/discount/file.xml", xmlName, discountPath));
        assertFalse(ListS3Extended.matchesFilters(
                "NIFIDEV/DATA/GOODWILL/Output/discount/file.txt", xmlName, discountPath));
        assertFalse(ListS3Extended.matchesFilters(
                "NIFIDEV/DATA/other/file.xml", xmlName, discountPath));
        assertTrue(ListS3Extended.matchesFilters(
                "NIFIDEV/DATA/GOODWILL/Output/discount/file.xml", xmlName, null));
    }
}
