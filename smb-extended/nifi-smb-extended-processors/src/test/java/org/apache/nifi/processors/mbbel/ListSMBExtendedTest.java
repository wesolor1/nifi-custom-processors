package org.apache.nifi.processors.mbbel;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.services.smb.SmbClientProviderService;
import org.apache.nifi.services.smb.SmbClientService;
import org.apache.nifi.services.smb.SmbListableEntity;
import org.apache.nifi.services.smb.SmbjClientProviderServiceExtended;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ListSMBExtendedTest {

    private static class EmptyListingSmb extends ListSMBExtended {
        @Override
        protected Integer countUnfilteredListing(final ProcessContext context) {
            return 0;
        }

        @Override
        protected List<SmbListableEntity> performListing(final ProcessContext context, final Long minimumTimestampOrNull,
                                                        final org.apache.nifi.processor.util.list.AbstractListProcessor.ListingMode listingMode) throws IOException {
            return Collections.emptyList();
        }
    }

    /** Remote directory contains entries, but none survive file/path/age/size filters. */
    private static class FilterExcludedListingSmb extends ListSMBExtended {
        @Override
        protected Integer countUnfilteredListing(final ProcessContext context) {
            return 1;
        }

        @Override
        protected List<SmbListableEntity> performListing(final ProcessContext context, final Long minimumTimestampOrNull,
                                                        final org.apache.nifi.processor.util.list.AbstractListProcessor.ListingMode listingMode) throws IOException {
            return Collections.emptyList();
        }
    }

    private static class ExceptionListingSmb extends ListSMBExtended {
        @Override
        protected List<SmbListableEntity> performListing(final ProcessContext context, final Long minimumTimestampOrNull,
                                                        final org.apache.nifi.processor.util.list.AbstractListProcessor.ListingMode listingMode) throws IOException {
            try {
                final java.lang.reflect.Field listingFailure = ListSMBExtended.class.getDeclaredField("listingFailure");
                listingFailure.setAccessible(true);
                @SuppressWarnings("unchecked")
                final ThreadLocal<Exception> failure = (ThreadLocal<Exception>) listingFailure.get(this);
                failure.set(new IOException("simulated smb listing failure"));
            } catch (final ReflectiveOperationException e) {
                throw new IOException("could not initialize listing failure in test", e);
            }
            throw new IOException("simulated smb listing failure");
        }

        @Override
        protected Integer countUnfilteredListing(final ProcessContext context) {
            return 0;
        }
    }

    /** Returns one fake {@link SmbListableEntity} so we can exercise the success route without real SMB I/O. */
    private static class SingleFileListingSmb extends ListSMBExtended {
        @Override
        protected Integer countUnfilteredListing(final ProcessContext context) {
            return 1;
        }

        @Override
        protected List<SmbListableEntity> performListing(final ProcessContext context, final Long minimumTimestampOrNull,
                                                        final org.apache.nifi.processor.util.list.AbstractListProcessor.ListingMode listingMode) {
            final SmbListableEntity entity = SmbListableEntity.builder()
                    .setName("test.txt")
                    .setShortName("test.txt")
                    .setPath("sub/folder")
                    .setDirectory(false)
                    .setSize(456L)
                    .setAllocationSize(512L)
                    // back-date so we satisfy MINIMUM_AGE default (5 secs).
                    .setLastModifiedTime(System.currentTimeMillis() - 60_000L)
                    .setCreationTime(System.currentTimeMillis() - 60_000L)
                    .setLastAccessTime(System.currentTimeMillis() - 60_000L)
                    .setChangeTime(System.currentTimeMillis() - 60_000L)
                    .setServiceLocation(URI.create("smb://localhost:445/share"))
                    .build();
            return List.of(entity);
        }
    }

    private static class MockSmbClientProviderService extends AbstractControllerService implements SmbClientProviderService {
        @Override
        public URI getServiceLocation() {
            return URI.create("smb://localhost:445/share");
        }

        @Override
        public SmbClientService getClient(final ComponentLog logger) throws IOException {
            throw new IOException("not used by these tests");
        }
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
                public void beginRecordSet() throws IOException {}

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

        assertTrue(new ListSMBExtended().getRelationships().contains(ListSMBExtended.REL_NO_FILES));
        assertEquals(0, result.getRecordCount());
        assertEquals("application/test", writer.getMimeType());
    }

    @Test
    public void testListingFailureProducesFailureFlowFilesAndIncomingTransferred() throws Exception {
        assertTrue(new ListSMBExtended().getRelationships().contains(ListSMBExtended.REL_FAILURE));
    }

    @Test
    public void testIncomingConnectionsAreAllowed() {
        final InputRequirement requirement = ListSMBExtended.class.getAnnotation(InputRequirement.class);
        assertNotNull(requirement, "@InputRequirement should be present on ListSMBExtended");
        assertEquals(Requirement.INPUT_ALLOWED, requirement.value());
    }

    @Test
    public void testProcessorPropertiesSupportFlowFileAttributesExpressionLanguage() {
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                ListSMBExtended.DIRECTORY.getExpressionLanguageScope(),
                "Input Directory must be evaluatable against FlowFile attributes");
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                ListSMBExtended.FILE_FILTER.getExpressionLanguageScope(),
                "File Filter must be evaluatable against FlowFile attributes");
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                ListSMBExtended.PATH_FILTER.getExpressionLanguageScope(),
                "Path Filter must be evaluatable against FlowFile attributes");
    }

    @Test
    public void testSmbClientProviderConnectionPropertiesSupportFlowFileAttributesExpressionLanguage() {
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                SmbjClientProviderServiceExtended.HOSTNAME.getExpressionLanguageScope(),
                "Hostname must be evaluatable against FlowFile attributes");
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                SmbjClientProviderServiceExtended.PORT.getExpressionLanguageScope(),
                "Port must be evaluatable against FlowFile attributes");
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                SmbjClientProviderServiceExtended.USERNAME.getExpressionLanguageScope(),
                "Username must be evaluatable against FlowFile attributes");
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                SmbjClientProviderServiceExtended.PASSWORD.getExpressionLanguageScope(),
                "Password must be evaluatable against FlowFile attributes");
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                SmbjClientProviderServiceExtended.SHARE.getExpressionLanguageScope(),
                "Share must be evaluatable against FlowFile attributes");
    }

    @Test
    public void testSmbClientProviderPasswordIsNotSensitive() {
        assertFalse(SmbjClientProviderServiceExtended.PASSWORD.isSensitive(),
                "SmbjClientProviderServiceExtended PASSWORD descriptor should be non-sensitive");
    }

    @Test
    public void testFileAndPathFilterAcceptExpressionLanguageWithoutValidationError() throws Exception {
        final TestRunner runner = newRunner(new ListSMBExtended());
        runner.setProperty(ListSMBExtended.FILE_FILTER, "${file_filter}");
        runner.setProperty(ListSMBExtended.PATH_FILTER, "${path_filter}");
        // Previously this produced "Not a valid Java Regular Expression" because the literal EL was compiled at config time.
        runner.assertValid();
    }

    @Test
    public void testInvalidLiteralRegexFilterIsRejected() throws Exception {
        final TestRunner runner = newRunner(new ListSMBExtended());
        runner.setProperty(ListSMBExtended.FILE_FILTER, "[");
        runner.assertNotValid();
    }

    @Test
    public void testBlankResolvedFilterIsIgnored() throws Exception {
        final TestRunner runner = newRunner(new ListSMBExtended());
        runner.setProperty(ListSMBExtended.FILE_FILTER, "${file_filter}");
        final ProcessContext context = runner.getProcessContext();
        final org.apache.nifi.components.PropertyValue fileFilter = context.getProperty(ListSMBExtended.FILE_FILTER);

        // Missing or empty attribute -> filter resolves to empty -> no filter applied.
        assertNull(ListSMBExtended.compileFilterOrNull(fileFilter, Map.of()),
                "Missing attribute should resolve to no filter");
        assertNull(ListSMBExtended.compileFilterOrNull(fileFilter, Map.of("file_filter", "")),
                "Empty attribute value should resolve to no filter");
        // Present attribute -> a compiled pattern is returned.
        assertNotNull(ListSMBExtended.compileFilterOrNull(fileFilter, Map.of("file_filter", "te.*")),
                "Non-empty attribute value should produce a compiled pattern");
    }

    // -----------------------------------------------------------------
    // Runtime behavior tests (drive onTrigger with TestRunner)
    // -----------------------------------------------------------------

    private static TestRunner newRunner(final ListSMBExtended processor) throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(processor);

        final MockSmbClientProviderService smbProvider = new MockSmbClientProviderService();
        runner.addControllerService("smb-provider", smbProvider);
        runner.enableControllerService(smbProvider);

        runner.setProperty(ListSMBExtended.SMB_CLIENT_PROVIDER_SERVICE, "smb-provider");
        runner.setProperty(ListSMBExtended.SMB_LISTING_STRATEGY, AbstractListProcessor.NO_TRACKING.getValue());
        return runner;
    }

    private static void registerWriter(final TestRunner runner) throws Exception {
        final MockWriterFactory writerFactory = new MockWriterFactory();
        runner.addControllerService("writer", writerFactory);
        runner.enableControllerService(writerFactory);
        runner.setProperty(AbstractListProcessor.RECORD_WRITER, "writer");
    }

    @Test
    public void testNoFilesRouteEmitsFlowFileWithIncomingAttributes() throws Exception {
        final TestRunner runner = newRunner(new EmptyListingSmb());
        registerWriter(runner);

        runner.enqueue("trigger", Map.of("batch.id", "B1", "input.path", "ignored"));
        runner.run(1);

        runner.assertTransferCount(ListSMBExtended.REL_NO_FILES, 1);
        runner.assertTransferCount(ListSMBExtended.REL_SUCCESS, 0);
        runner.assertTransferCount(ListSMBExtended.REL_FAILURE, 0);

        final MockFlowFile emitted = runner.getFlowFilesForRelationship(ListSMBExtended.REL_NO_FILES).get(0);
        emitted.assertAttributeEquals("batch.id", "B1");
        emitted.assertAttributeEquals("record.count", "0");
        emitted.assertAttributeEquals("mime.type", "application/test");
        emitted.assertAttributeEquals("smb.service.location", "smb://localhost:445/share");
    }

    @Test
    public void testNoFilesRouteResolvesDirectoryExpressionLanguageFromIncomingAttributes() throws Exception {
        final TestRunner runner = newRunner(new EmptyListingSmb());
        runner.setProperty(ListSMBExtended.DIRECTORY, "${input.dir}");
        registerWriter(runner);

        runner.enqueue("trigger", Map.of("input.dir", "shared/incoming"));
        runner.run(1);

        final MockFlowFile emitted = runner.getFlowFilesForRelationship(ListSMBExtended.REL_NO_FILES).get(0);
        emitted.assertAttributeEquals("smb.listing.directory", "shared/incoming");
    }

    @Test
    public void testNoFilesRouteWhenAllEntriesFilteredOut() throws Exception {
        final TestRunner runner = newRunner(new FilterExcludedListingSmb());
        runner.setProperty(ListSMBExtended.FILE_FILTER, ".*\\.csv");
        registerWriter(runner);

        runner.enqueue("trigger", Map.of("batch.id", "B-filtered"));
        runner.run(1);

        runner.assertTransferCount(ListSMBExtended.REL_NO_FILES, 1);
        runner.assertTransferCount(ListSMBExtended.REL_SUCCESS, 0);
        runner.assertTransferCount(ListSMBExtended.REL_FAILURE, 0);

        final MockFlowFile emitted = runner.getFlowFilesForRelationship(ListSMBExtended.REL_NO_FILES).get(0);
        emitted.assertAttributeEquals("batch.id", "B-filtered");
        emitted.assertAttributeEquals("record.count", "0");
    }

    @Test
    public void testListingFailureRouteEmitsFailureFlowFileWithIncomingAttributes() throws Exception {
        final TestRunner runner = newRunner(new ExceptionListingSmb());

        runner.enqueue("trigger", Map.of("batch.id", "B2"));
        runner.run(1);

        runner.assertTransferCount(ListSMBExtended.REL_FAILURE, 1);
        runner.assertTransferCount(ListSMBExtended.REL_SUCCESS, 0);
        runner.assertTransferCount(ListSMBExtended.REL_NO_FILES, 0);

        final MockFlowFile failure = runner.getFlowFilesForRelationship(ListSMBExtended.REL_FAILURE).get(0);
        failure.assertAttributeEquals("batch.id", "B2");
        failure.assertAttributeEquals("error.message", "simulated smb listing failure");
        failure.assertAttributeExists("error.class");
        failure.assertAttributeEquals("smb.service.location", "smb://localhost:445/share");
    }

    @Test
    public void testSuccessRouteCopiesIncomingAttributes() throws Exception {
        final TestRunner runner = newRunner(new SingleFileListingSmb());

        runner.enqueue("trigger", Map.of("batch.id", "B3"));
        runner.run(1);

        runner.assertTransferCount(ListSMBExtended.REL_SUCCESS, 1);
        runner.assertTransferCount(ListSMBExtended.REL_NO_FILES, 0);
        runner.assertTransferCount(ListSMBExtended.REL_FAILURE, 0);

        final MockFlowFile emitted = runner.getFlowFilesForRelationship(ListSMBExtended.REL_SUCCESS).get(0);
        emitted.assertAttributeEquals("batch.id", "B3");
        emitted.assertAttributeEquals("filename", "test.txt");
        emitted.assertAttributeEquals("path", "sub/folder");
    }

    @Test
    public void testTriggerFlowFileIsRemovedFromSession() throws Exception {
        final TestRunner runner = newRunner(new EmptyListingSmb());

        runner.enqueue("trigger", Map.of("batch.id", "B4"));
        runner.run(1);

        runner.assertQueueEmpty();
        runner.assertTransferCount(ListSMBExtended.REL_SUCCESS, 0);
        runner.assertTransferCount(ListSMBExtended.REL_NO_FILES, 0);
        runner.assertTransferCount(ListSMBExtended.REL_FAILURE, 0);
    }
}

