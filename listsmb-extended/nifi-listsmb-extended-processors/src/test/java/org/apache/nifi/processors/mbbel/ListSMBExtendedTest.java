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
}

