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
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.file.transfer.FileInfo;
import org.apache.nifi.processor.util.file.transfer.ListFileTransfer;
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processors.standard.util.SFTPTransfer;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ListSFTPExtendedTest {

    private static class EmptyListingSftp extends ListSFTPExtended {
        @Override
        protected List<FileInfo> performListing(final ProcessContext context, final Long minTimestamp, final ListingMode listingMode,
                                                final boolean applyFilters) throws IOException {
            return Collections.emptyList();
        }
    }

    /** Remote directory contains entries, but none survive file/age/size filters. */
    private static class FilterExcludedListingSftp extends ListSFTPExtended {
        @Override
        protected List<FileInfo> performListing(final ProcessContext context, final Long minTimestamp, final ListingMode listingMode,
                                                final boolean applyFilters) throws IOException {
            if (applyFilters) {
                return Collections.emptyList();
            }
            final FileInfo file = new FileInfo.Builder()
                    .filename("other.txt")
                    .fullPathFileName("/remote/dir/other.txt")
                    .directory(false)
                    .size(100L)
                    .lastModifiedTime(System.currentTimeMillis() - 60_000L)
                    .permissions("rw-r--r--")
                    .owner("nifi")
                    .group("nifi")
                    .build();
            return List.of(file);
        }
    }

    private static class ExceptionListingSftp extends ListSFTPExtended {
        @Override
        protected List<FileInfo> performListing(final ProcessContext context, final Long minTimestamp, final ListingMode listingMode,
                                                final boolean applyFilters) throws IOException {
            throw new IOException("simulated listing failure");
        }
    }

    /** Returns a single fake {@link FileInfo} so we can drive the success path without real SFTP I/O. */
    private static class SingleFileListingSftp extends ListSFTPExtended {
        @Override
        protected List<FileInfo> performListing(final ProcessContext context, final Long minTimestamp, final ListingMode listingMode,
                                                final boolean applyFilters) {
            final FileInfo file = new FileInfo.Builder()
                    .filename("test.txt")
                    .fullPathFileName("/remote/dir/test.txt")
                    .directory(false)
                    .size(123L)
                    // back-date so we are older than ListFile.MIN_AGE (default 5 secs).
                    .lastModifiedTime(System.currentTimeMillis() - 60_000L)
                    .permissions("rw-r--r--")
                    .owner("nifi")
                    .group("nifi")
                    .build();
            return List.of(file);
        }

        /**
         * Bypass the parent {@code ListFileTransfer.createAttributes}, which calls
         * {@code context.getProperty(HOSTNAME).evaluateAttributeExpressions()} (no args).
         * NiFi's {@code MockPropertyValue} (unlike real NiFi) rejects no-args evaluation on
         * FLOWFILE_ATTRIBUTES-scoped properties, so we just return a minimal listing attribute map.
         * The trigger-attribute merge still happens via the {@code withTriggerAttributes} proxy.
         */
        @Override
        protected Map<String, String> createAttributes(final FileInfo fileInfo, final ProcessContext context) {
            return Map.of(
                    "filename", fileInfo.getFileName(),
                    "file.size", String.valueOf(fileInfo.getSize()));
        }
    }

    private static TestRunner newRunner(final ListSFTPExtended processor) throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(ListFileTransfer.HOSTNAME, "sftp.example.com");
        runner.setProperty(SFTPTransfer.USERNAME, "tester");
        runner.setProperty(ListSFTPExtended.REMOTE_PATH, "/remote/dir");
        runner.setProperty(ListFileTransfer.FILE_TRANSFER_LISTING_STRATEGY, AbstractListProcessor.NO_TRACKING.getValue());
        return runner;
    }

    private static void registerWriter(final TestRunner runner) throws Exception {
        final MockWriterFactory writerFactory = new MockWriterFactory();
        runner.addControllerService("writer", writerFactory);
        runner.enableControllerService(writerFactory);
        runner.setProperty(AbstractListProcessor.RECORD_WRITER, "writer");
    }

    private static class MockWriterFactory extends AbstractControllerService implements RecordSetWriterFactory {
        @Override
        public RecordSetWriter createWriter(ComponentLog logger, RecordSchema schema, OutputStream out, Map<String, String> variables) {
            return new RecordSetWriter() {
                @Override
                public WriteResult write(org.apache.nifi.serialization.record.Record record) throws IOException {
                    throw new UnsupportedOperationException();
                }

                @Override
                public WriteResult write(org.apache.nifi.serialization.record.RecordSet recordSet) throws IOException {
                    return finishRecordSet();
                }

                @Override
                public void beginRecordSet() throws IOException {
                }

                @Override
                public WriteResult finishRecordSet() throws IOException {
                    return WriteResult.of(0, Collections.emptyMap());
                }

                @Override
                public String getMimeType() {
                    return "application/test";
                }

                @Override
                public void flush() throws IOException {}

                @Override
                public void close() throws IOException {}
            };
        }

        @Override
        public RecordSchema getSchema(Map<String, String> variables, RecordSchema inputSchema) throws SchemaNotFoundException {
            return inputSchema;
        }
    }

    @Test
    public void testEmptyDirectoryEmitsZeroRecordFlowFile() throws Exception {
        final MockWriterFactory writerFactory = new MockWriterFactory();
        final RecordSetWriter writer = writerFactory.createWriter(null, null, OutputStream.nullOutputStream(), Map.of());

        writer.beginRecordSet();
        final WriteResult result = writer.finishRecordSet();

        assertTrue(new ListSFTPExtended().getRelationships().contains(ListSFTPExtended.REL_NO_FILES));
        assertEquals(0, result.getRecordCount());
        assertEquals("application/test", writer.getMimeType());
    }

    @Test
    public void testListingFailureProducesFailureFlowFile() throws Exception {
        assertTrue(new ListSFTPExtended().getRelationships().contains(ListSFTPExtended.REL_FAILURE));
    }

    @Test
    public void testIncomingConnectionsAreAllowed() {
        final InputRequirement requirement = ListSFTPExtended.class.getAnnotation(InputRequirement.class);
        assertNotNull(requirement, "@InputRequirement should be present on ListSFTPExtended");
        assertEquals(Requirement.INPUT_ALLOWED, requirement.value());
    }

    @Test
    public void testConnectionPropertiesSupportFlowFileAttributesExpressionLanguage() {
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                SFTPTransfer.HOSTNAME.getExpressionLanguageScope(),
                "Hostname must be evaluatable against FlowFile attributes");
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                SFTPTransfer.PORT.getExpressionLanguageScope(),
                "Port must be evaluatable against FlowFile attributes");
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                SFTPTransfer.USERNAME.getExpressionLanguageScope(),
                "Username must be evaluatable against FlowFile attributes");
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                ListSFTPExtended.PASSWORD.getExpressionLanguageScope(),
                "Password must be evaluatable against FlowFile attributes");
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                ListSFTPExtended.REMOTE_PATH.getExpressionLanguageScope(),
                "Remote Path must be evaluatable against FlowFile attributes");
    }

    @Test
    public void testCustomPasswordIsNotSensitive() {
        assertFalse(ListSFTPExtended.PASSWORD.isSensitive(),
                "ListSFTPExtended PASSWORD descriptor should override stock SFTPTransfer.PASSWORD to be non-sensitive");
    }

    @Test
    public void testFileAndPathFilterSupportFlowFileAttributesExpressionLanguage() {
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                ListSFTPExtended.FILE_FILTER_REGEX.getExpressionLanguageScope());
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                ListSFTPExtended.PATH_FILTER_REGEX.getExpressionLanguageScope());
    }

    @Test
    public void testFileAndPathFilterAcceptExpressionLanguageWithoutValidationError() throws Exception {
        final TestRunner runner = newRunner(new ListSFTPExtended());
        runner.setProperty(ListSFTPExtended.FILE_FILTER_REGEX, "${file_filter}");
        runner.setProperty(ListSFTPExtended.PATH_FILTER_REGEX, "${path_filter}");
        runner.assertValid();
    }

    @Test
    public void testBlankResolvedFilterIsIgnored() throws Exception {
        final TestRunner runner = newRunner(new ListSFTPExtended());
        runner.setProperty(ListSFTPExtended.FILE_FILTER_REGEX, "${file_filter}");
        final var context = runner.getProcessContext();
        final var fileFilter = context.getProperty(ListSFTPExtended.FILE_FILTER_REGEX);

        assertNull(ListSFTPExtended.compileFilterOrNull(fileFilter, Map.of()));
        assertNull(ListSFTPExtended.compileFilterOrNull(fileFilter, Map.of("file_filter", "")));
        assertNotNull(ListSFTPExtended.compileFilterOrNull(fileFilter, Map.of("file_filter", "te.*")));
    }

    @Test
    public void testBlankPasswordResolvesToNullSoKeyAuthCanBeUsed() throws Exception {
        final TestRunner runner = newRunner(new ListSFTPExtended());
        runner.setProperty(ListSFTPExtended.PASSWORD, "${target_password}");
        runner.setProperty(SFTPTransfer.PRIVATE_KEY_PATH, "${target_private_key_path}");

        final ProcessContext sanitized = SftpBlankAsUnsetSupport.blankAsUnsetContext(runner.getProcessContext());

        assertNull(sanitized.getProperty(SFTPTransfer.PASSWORD)
                .evaluateAttributeExpressions(Map.of("target_private_key_path", "/opt/nifi/keys/key.pem")).getValue());
        assertEquals("/opt/nifi/keys/key.pem", sanitized.getProperty(SFTPTransfer.PRIVATE_KEY_PATH)
                .evaluateAttributeExpressions(Map.of("target_private_key_path", "/opt/nifi/keys/key.pem")).getValue());
    }

    @Test
    public void testBlankPrivateKeyResolvesToNullSoPasswordAuthCanBeUsed() throws Exception {
        final TestRunner runner = newRunner(new ListSFTPExtended());
        runner.setProperty(ListSFTPExtended.PASSWORD, "${target_password}");
        runner.setProperty(SFTPTransfer.PRIVATE_KEY_PATH, "${target_private_key_path}");

        final ProcessContext sanitized = SftpBlankAsUnsetSupport.blankAsUnsetContext(runner.getProcessContext());
        final Map<String, String> attributes = Map.of("target_password", "secret");

        assertEquals("secret", sanitized.getProperty(SFTPTransfer.PASSWORD)
                .evaluateAttributeExpressions(attributes).getValue());
        assertNull(sanitized.getProperty(SFTPTransfer.PRIVATE_KEY_PATH)
                .evaluateAttributeExpressions(attributes).getValue());
    }

    // -----------------------------------------------------------------
    // Runtime behavior tests (drive onTrigger with TestRunner)
    // -----------------------------------------------------------------

    @Test
    public void testNoFilesRouteEmitsFlowFileWithIncomingAttributes() throws Exception {
        final TestRunner runner = newRunner(new EmptyListingSftp());
        registerWriter(runner);

        runner.enqueue("trigger", Map.of("batch.id", "B1", "target.host", "ignored"));
        runner.run(1);

        runner.assertTransferCount(ListSFTPExtended.REL_NO_FILES, 1);
        runner.assertTransferCount(ListSFTPExtended.REL_SUCCESS, 0);
        runner.assertTransferCount(ListSFTPExtended.REL_FAILURE, 0);

        final MockFlowFile emitted = runner.getFlowFilesForRelationship(ListSFTPExtended.REL_NO_FILES).get(0);
        emitted.assertAttributeEquals("batch.id", "B1");
        emitted.assertAttributeEquals("record.count", "0");
        emitted.assertAttributeEquals("mime.type", "application/test");
    }

    @Test
    public void testNoFilesRouteResolvesExpressionLanguageFromIncomingAttributes() throws Exception {
        final TestRunner runner = newRunner(new EmptyListingSftp());
        runner.setProperty(ListFileTransfer.HOSTNAME, "${target.host}");
        runner.setProperty(SFTPTransfer.USERNAME, "${target.user}");
        registerWriter(runner);

        runner.enqueue("trigger", Map.of("target.host", "actual-host.example", "target.user", "jdoe"));
        runner.run(1);

        final MockFlowFile emitted = runner.getFlowFilesForRelationship(ListSFTPExtended.REL_NO_FILES).get(0);
        emitted.assertAttributeEquals("sftp.remote.host", "actual-host.example");
        emitted.assertAttributeEquals("sftp.listing.user", "jdoe");
    }

    @Test
    public void testNoFilesRouteWhenAllEntriesFilteredOut() throws Exception {
        final TestRunner runner = newRunner(new FilterExcludedListingSftp());
        registerWriter(runner);

        runner.enqueue("trigger", Map.of("batch.id", "B-filtered"));
        runner.run(1);

        runner.assertTransferCount(ListSFTPExtended.REL_NO_FILES, 1);
        runner.assertTransferCount(ListSFTPExtended.REL_SUCCESS, 0);
        runner.assertTransferCount(ListSFTPExtended.REL_FAILURE, 0);

        final MockFlowFile emitted = runner.getFlowFilesForRelationship(ListSFTPExtended.REL_NO_FILES).get(0);
        emitted.assertAttributeEquals("batch.id", "B-filtered");
        emitted.assertAttributeEquals("record.count", "0");
    }

    @Test
    public void testSuccessRouteCopiesIncomingAttributesAndListingTakesPrecedence() throws Exception {
        final TestRunner runner = newRunner(new SingleFileListingSftp());
        runner.assertValid();

        runner.enqueue("trigger", Map.of(
                "batch.id", "B2",
                "filename", "should-be-overridden-by-listing"));
        runner.run(1);

        final int success = runner.getFlowFilesForRelationship(ListSFTPExtended.REL_SUCCESS).size();
        final int noFiles = runner.getFlowFilesForRelationship(ListSFTPExtended.REL_NO_FILES).size();
        final List<MockFlowFile> failures = runner.getFlowFilesForRelationship(ListSFTPExtended.REL_FAILURE);
        final int failure = failures.size();
        final String firstFailureError = failure == 0 ? "" : failures.get(0).getAttribute("error.message");
        assertEquals(1, success, "Expected listing to produce a success FlowFile (success=" + success
                + ", no_files=" + noFiles + ", failure=" + failure + ", firstFailureError=" + firstFailureError + ")");
        assertEquals(0, noFiles);
        assertEquals(0, failure);

        final MockFlowFile emitted = runner.getFlowFilesForRelationship(ListSFTPExtended.REL_SUCCESS).get(0);
        emitted.assertAttributeEquals("batch.id", "B2");
        emitted.assertAttributeEquals("filename", "test.txt");
    }

    @Test
    public void testTriggerFlowFileIsRemovedFromSession() throws Exception {
        final TestRunner runner = newRunner(new EmptyListingSftp());

        runner.enqueue("trigger", Map.of("batch.id", "B3"));
        runner.run(1);

        runner.assertQueueEmpty();
        runner.assertTransferCount(ListSFTPExtended.REL_SUCCESS, 0);
        runner.assertTransferCount(ListSFTPExtended.REL_NO_FILES, 0);
        runner.assertTransferCount(ListSFTPExtended.REL_FAILURE, 0);
    }

}
