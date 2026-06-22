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
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.azure.storage.ListAzureDataLakeStorage;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.services.azure.storage.ADLSCredentialsDetails;
import org.apache.nifi.services.azure.storage.ADLSCredentialsService;
import org.apache.nifi.services.azure.storage.ADLSCredentialsControllerServiceExtended;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.evaluateFileSystemProperty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ListAzureDataLakeStorageExtendedTest {

    private static final String CREDENTIALS_SERVICE_ID = "adls-creds";

    /**
     * Minimal stub so the processor validates inside a TestRunner without requiring real Azure credentials.
     * The credential details are never requested because the test double below never contacts Azure.
     */
    public static class StubCredentialsService extends AbstractControllerService implements ADLSCredentialsService {
        @Override
        public ADLSCredentialsDetails getCredentialsDetails(final Map<String, String> attributes) {
            throw new UnsupportedOperationException("Tests should not contact Azure");
        }
    }

    /**
     * Test double that records what context/session the listing was invoked with and emits a single listing FlowFile
     * (mimicking the per-entity FlowFile creation of AbstractListProcessor), without contacting Azure. The filesystem
     * value is resolved through the production helper so we can assert Expression Language is evaluated against the
     * trigger FlowFile attributes.
     */
    public static class TestableListAzureDataLakeStorageExtended extends ListAzureDataLakeStorageExtended {
        volatile boolean listingPerformed = false;
        volatile String resolvedFileSystem;
        volatile boolean emitFlowFile = true;
        volatile boolean hasMatchingResults = true;
        volatile RuntimeException listingException;

        @Override
        protected boolean hasMatchingListingResults(final ProcessContext context) {
            return hasMatchingResults;
        }

        @Override
        protected void runListing(final ProcessContext context, final ProcessSession session) {
            listingPerformed = true;
            if (listingException != null) {
                throw listingException;
            }
            resolvedFileSystem = evaluateFileSystemProperty(AzureStorageUtils.FILESYSTEM, context, getTriggerFlowAttributes());

            if (!emitFlowFile) {
                return;
            }

            FlowFile listed = session.create();
            final Map<String, String> attributes = new HashMap<>();
            attributes.put("filename", "data-file.csv");
            attributes.put("azure.filesystem", resolvedFileSystem);
            // Intentionally also set an attribute that may collide with a trigger attribute to prove precedence.
            attributes.put("origin", "listing");
            listed = session.putAllAttributes(listed, attributes);
            session.transfer(listed, REL_SUCCESS);
        }
    }

    private TestRunner newRunner(final ListAzureDataLakeStorageExtended processor) throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(processor);
        final StubCredentialsService credentials = new StubCredentialsService();
        runner.addControllerService(CREDENTIALS_SERVICE_ID, credentials);
        runner.enableControllerService(credentials);
        runner.setProperty(AzureStorageUtils.ADLS_CREDENTIALS_SERVICE, CREDENTIALS_SERVICE_ID);
        runner.setProperty(AzureStorageUtils.DIRECTORY, "data");
        return runner;
    }

    private PropertyDescriptor exposedDescriptor(final ListAzureDataLakeStorageExtended processor, final String name) {
        return processor.getPropertyDescriptors().stream()
                .filter(pd -> pd.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Descriptor not exposed by processor: " + name));
    }

    @Test
    public void testIncomingConnectionsAreAllowed() {
        final InputRequirement requirement = ListAzureDataLakeStorageExtended.class.getAnnotation(InputRequirement.class);
        assertNotNull(requirement, "@InputRequirement should be present on ListAzureDataLakeStorageExtended");
        assertEquals(Requirement.INPUT_ALLOWED, requirement.value(),
                "ListAzureDataLakeStorageExtended must allow incoming connections (stock processor forbids them)");
    }

    @Test
    public void testPrimaryNodeOnlyIsNotInherited() {
        // @PrimaryNodeOnly is @Inherited, which is why this processor extends AbstractListAzureProcessor directly
        // instead of subclassing the stock ListAzureDataLakeStorage.
        assertNull(ListAzureDataLakeStorageExtended.class.getAnnotation(PrimaryNodeOnly.class),
                "ListAzureDataLakeStorageExtended must not be primary-node-only so trigger FlowFiles can be processed on any node");
    }

    @Test
    public void testFilesystemAndDirectorySupportFlowFileAttributesExpressionLanguage() {
        final ListAzureDataLakeStorageExtended processor = new ListAzureDataLakeStorageExtended();
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                exposedDescriptor(processor, AzureStorageUtils.FILESYSTEM.getName()).getExpressionLanguageScope(),
                "Filesystem Name must be evaluatable against FlowFile attributes");
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                exposedDescriptor(processor, AzureStorageUtils.DIRECTORY.getName()).getExpressionLanguageScope(),
                "Directory Name must be evaluatable against FlowFile attributes");
    }

    @Test
    public void testPropertiesMatchStockListAzureDataLakeStorage() {
        final List<PropertyDescriptor> base = new ListAzureDataLakeStorage().getPropertyDescriptors();
        final List<PropertyDescriptor> extended = new ListAzureDataLakeStorageExtended().getPropertyDescriptors();

        assertEquals(base.size(), extended.size(), "Extended processor must expose the same set of properties");
        for (int i = 0; i < base.size(); i++) {
            final PropertyDescriptor baseDescriptor = base.get(i);
            final PropertyDescriptor extendedDescriptor = extended.get(i);
            assertEquals(baseDescriptor.getName(), extendedDescriptor.getName(),
                    "Property ordering and names must match the stock processor");
            if (ListAzureDataLakeStorageExtended.FILE_FILTER.getName().equals(baseDescriptor.getName())
                    || ListAzureDataLakeStorageExtended.PATH_FILTER.getName().equals(baseDescriptor.getName())) {
                assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES, extendedDescriptor.getExpressionLanguageScope(),
                        "Filter properties must support FlowFile-attribute Expression Language");
            } else {
                assertEquals(baseDescriptor, extendedDescriptor, "Property definitions must match the stock processor");
            }
        }
    }

    @Test
    public void testFileFilterSupportsFlowFileAttributesExpressionLanguage() {
        final ListAzureDataLakeStorageExtended processor = new ListAzureDataLakeStorageExtended();
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                exposedDescriptor(processor, ListAzureDataLakeStorageExtended.FILE_FILTER.getName()).getExpressionLanguageScope());
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                exposedDescriptor(processor, ListAzureDataLakeStorageExtended.PATH_FILTER.getName()).getExpressionLanguageScope());
    }

    @Test
    public void testFileFilterExpressionLanguageResolvedFromTriggerAttributes() throws InitializationException {
        final TestRunner runner = newRunner(new ListAzureDataLakeStorageExtended());
        runner.setProperty(AzureStorageUtils.FILESYSTEM, "my-filesystem");
        runner.setProperty(ListAzureDataLakeStorageExtended.FILE_FILTER, "${source_trigger_filename}");

        final PropertyValue fileFilter = runner.getProcessContext().getProperty(ListAzureDataLakeStorageExtended.FILE_FILTER);
        assertNull(ListAzureDataLakeStorageExtended.compileFilterOrNull(fileFilter, Map.of()));
        assertNull(ListAzureDataLakeStorageExtended.compileFilterOrNull(fileFilter, Map.of("source_trigger_filename", "")));
        assertNotNull(ListAzureDataLakeStorageExtended.compileFilterOrNull(fileFilter, Map.of("source_trigger_filename", "^export.done")));
        assertTrue(ListAzureDataLakeStorageExtended.compileFilterOrNull(fileFilter, Map.of("source_trigger_filename", "^export.done"))
                .matcher("export.done").matches());
    }

    @Test
    public void testRelationshipsIncludeStockPlusFailureAndNoFiles() {
        final ListAzureDataLakeStorageExtended extended = new ListAzureDataLakeStorageExtended();
        assertTrue(extended.getRelationships().containsAll(new ListAzureDataLakeStorage().getRelationships()),
                "Extended processor should expose all stock relationships");
        assertTrue(extended.getRelationships().contains(ListAzureDataLakeStorageExtended.REL_FAILURE),
                "Extended processor should expose the Failure relationship");
        assertTrue(extended.getRelationships().contains(ListAzureDataLakeStorageExtended.REL_NO_FILES),
                "Extended processor should expose the No Files relationship");
    }

    @Test
    public void testTriggerFlowFileIsConsumedAndAttributesCopiedToListing() throws InitializationException {
        final TestableListAzureDataLakeStorageExtended processor = new TestableListAzureDataLakeStorageExtended();
        final TestRunner runner = newRunner(processor);
        runner.setProperty(AzureStorageUtils.FILESYSTEM, "my-filesystem");

        final Map<String, String> triggerAttributes = new HashMap<>();
        triggerAttributes.put("batch.id", "12345");
        triggerAttributes.put("origin", "trigger");
        runner.enqueue("trigger".getBytes(), triggerAttributes);

        runner.run();

        assertTrue(processor.listingPerformed, "Listing should have been performed on the incoming trigger");
        runner.assertQueueEmpty();
        runner.assertTransferCount(ListAzureDataLakeStorageExtended.REL_SUCCESS, 1);

        final MockFlowFile out = runner.getFlowFilesForRelationship(ListAzureDataLakeStorageExtended.REL_SUCCESS).get(0);
        out.assertAttributeEquals("filename", "data-file.csv");
        out.assertAttributeEquals("batch.id", "12345");
        // Listing attribute wins over the trigger attribute on name collision.
        out.assertAttributeEquals("origin", "listing");
    }

    @Test
    public void testFilesystemExpressionLanguageResolvedFromTriggerAttributes() throws InitializationException {
        final TestableListAzureDataLakeStorageExtended processor = new TestableListAzureDataLakeStorageExtended();
        final TestRunner runner = newRunner(processor);
        runner.setProperty(AzureStorageUtils.FILESYSTEM, "${target.filesystem}");

        final Map<String, String> triggerAttributes = new HashMap<>();
        triggerAttributes.put("target.filesystem", "resolved-filesystem");
        runner.enqueue("trigger".getBytes(), triggerAttributes);

        runner.run();

        assertEquals("resolved-filesystem", processor.resolvedFileSystem,
                "Filesystem Name Expression Language should resolve against the trigger FlowFile attributes");
        final MockFlowFile out = runner.getFlowFilesForRelationship(ListAzureDataLakeStorageExtended.REL_SUCCESS).get(0);
        out.assertAttributeEquals("azure.filesystem", "resolved-filesystem");
    }

    @Test
    public void testListingFailureRoutesErrorFlowFileWithTriggerAttributes() throws InitializationException {
        final TestableListAzureDataLakeStorageExtended processor = new TestableListAzureDataLakeStorageExtended();
        processor.listingException = new RuntimeException("simulated ADLS failure");
        final TestRunner runner = newRunner(processor);
        runner.setProperty(AzureStorageUtils.FILESYSTEM, "my-filesystem");

        final Map<String, String> triggerAttributes = new HashMap<>();
        triggerAttributes.put("batch.id", "12345");
        runner.enqueue("trigger".getBytes(), triggerAttributes);

        runner.run();

        runner.assertQueueEmpty();
        runner.assertTransferCount(ListAzureDataLakeStorageExtended.REL_SUCCESS, 0);
        runner.assertTransferCount(ListAzureDataLakeStorageExtended.REL_FAILURE, 1);

        final MockFlowFile out = runner.getFlowFilesForRelationship(ListAzureDataLakeStorageExtended.REL_FAILURE).get(0);
        out.assertAttributeEquals("error.message", "simulated ADLS failure");
        out.assertAttributeEquals("error.class", RuntimeException.class.getName());
        out.assertAttributeEquals("azure.filesystem", "my-filesystem");
        out.assertAttributeEquals("batch.id", "12345");
    }

    @Test
    public void testEmptyDirectoryRoutesZeroRecordFlowFileToNoFiles() throws InitializationException {
        final TestableListAzureDataLakeStorageExtended processor = new TestableListAzureDataLakeStorageExtended();
        processor.emitFlowFile = false;
        processor.hasMatchingResults = false;
        final TestRunner runner = newRunner(processor);
        runner.setProperty(AzureStorageUtils.FILESYSTEM, "my-filesystem");

        final MockRecordWriter writerFactory = new MockRecordWriter(null, false);
        runner.addControllerService("record-writer", writerFactory);
        runner.enableControllerService(writerFactory);
        runner.setProperty(ListAzureDataLakeStorageExtended.RECORD_WRITER, "record-writer");

        final Map<String, String> triggerAttributes = new HashMap<>();
        triggerAttributes.put("batch.id", "12345");
        runner.enqueue("trigger".getBytes(), triggerAttributes);

        runner.run();

        runner.assertQueueEmpty();
        runner.assertTransferCount(ListAzureDataLakeStorageExtended.REL_SUCCESS, 0);
        runner.assertTransferCount(ListAzureDataLakeStorageExtended.REL_FAILURE, 0);
        runner.assertTransferCount(ListAzureDataLakeStorageExtended.REL_NO_FILES, 1);

        final MockFlowFile out = runner.getFlowFilesForRelationship(ListAzureDataLakeStorageExtended.REL_NO_FILES).get(0);
        out.assertAttributeEquals("record.count", "0");
        out.assertAttributeEquals("azure.filesystem", "my-filesystem");
        out.assertAttributeEquals("batch.id", "12345");
    }

    @Test
    public void testNoFilesIsNotEmittedWithoutRecordWriter() throws InitializationException {
        final TestableListAzureDataLakeStorageExtended processor = new TestableListAzureDataLakeStorageExtended();
        processor.emitFlowFile = false;
        processor.hasMatchingResults = false;
        final TestRunner runner = newRunner(processor);
        runner.setProperty(AzureStorageUtils.FILESYSTEM, "my-filesystem");

        runner.enqueue("trigger".getBytes());
        runner.run();

        runner.assertTransferCount(ListAzureDataLakeStorageExtended.REL_NO_FILES, 0);
        runner.assertTransferCount(ListAzureDataLakeStorageExtended.REL_FAILURE, 0);
    }

    @Test
    public void testNoFilesIsNotEmittedWhenListingProducesFiles() throws InitializationException {
        final TestableListAzureDataLakeStorageExtended processor = new TestableListAzureDataLakeStorageExtended();
        processor.hasMatchingResults = true;
        final TestRunner runner = newRunner(processor);
        runner.setProperty(AzureStorageUtils.FILESYSTEM, "my-filesystem");

        final MockRecordWriter writerFactory = new MockRecordWriter(null, false);
        runner.addControllerService("record-writer", writerFactory);
        runner.enableControllerService(writerFactory);
        runner.setProperty(ListAzureDataLakeStorageExtended.RECORD_WRITER, "record-writer");

        runner.enqueue("trigger".getBytes());
        runner.run();

        runner.assertTransferCount(ListAzureDataLakeStorageExtended.REL_SUCCESS, 1);
        runner.assertTransferCount(ListAzureDataLakeStorageExtended.REL_NO_FILES, 0);
    }

    @Test
    public void testSeeAlsoIncludesExtendedCredentialsService() {
        final SeeAlso seeAlso = ListAzureDataLakeStorageExtended.class.getAnnotation(SeeAlso.class);
        assertNotNull(seeAlso);
        assertTrue(java.util.Arrays.asList(seeAlso.value()).contains(ADLSCredentialsControllerServiceExtended.class));
    }

    @Test
    public void testEmptyIncomingConnectionYieldsWithoutListing() throws InitializationException {
        final TestableListAzureDataLakeStorageExtended processor = new TestableListAzureDataLakeStorageExtended();
        final TestRunner runner = newRunner(processor);
        runner.setProperty(AzureStorageUtils.FILESYSTEM, "my-filesystem");
        runner.setIncomingConnection(true);

        runner.run();

        assertFalse(processor.listingPerformed,
                "With an empty incoming connection the processor should yield instead of listing");
        runner.assertTransferCount(ListAzureDataLakeStorageExtended.REL_SUCCESS, 0);
    }
}
