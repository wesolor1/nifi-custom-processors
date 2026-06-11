package org.apache.nifi.processors.mbbel;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.services.smb.SmbClientProviderService;
import org.apache.nifi.services.smb.SmbClientService;
import org.apache.nifi.services.smb.SmbException;
import org.apache.nifi.services.smb.SmbListableEntity;
import org.apache.nifi.services.smb.SmbjClientProviderServiceExtended;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DeleteSmbExtendedTest {

    /**
     * Captures the evaluation-attributes ThreadLocal at {@code getClient} time and hands back a client
     * whose {@code deleteFile} behavior is configurable so we can exercise success / not-found / failure.
     */
    private static class CapturingProviderService extends AbstractControllerService implements SmbClientProviderService {

        private volatile Map<String, String> capturedEvaluationAttributes;
        private final RuntimeException deleteException;
        private volatile String deletedPath;

        private CapturingProviderService(final RuntimeException deleteException) {
            this.deleteException = deleteException;
        }

        @Override
        public URI getServiceLocation() {
            return URI.create("smb://localhost:445/share");
        }

        @Override
        @SuppressWarnings("unchecked")
        public SmbClientService getClient(final ComponentLog logger) throws IOException {
            try {
                final Field field = SmbjClientProviderServiceExtended.class.getDeclaredField("EVALUATION_ATTRIBUTES");
                field.setAccessible(true);
                final ThreadLocal<Map<String, String>> threadLocal = (ThreadLocal<Map<String, String>>) field.get(null);
                capturedEvaluationAttributes = threadLocal.get();
            } catch (final ReflectiveOperationException e) {
                throw new IOException("could not read evaluation attributes in test", e);
            }
            return new ConfigurableClient();
        }

        private class ConfigurableClient implements SmbClientService {
            @Override
            public Stream<SmbListableEntity> listFiles(final String directory) {
                return Stream.empty();
            }

            @Override
            public void ensureDirectory(final String directory) {
            }

            @Override
            public void readFile(final String fileName, final OutputStream outputStream) {
            }

            @Override
            public void moveFile(final String fileName, final String targetDirectory) {
            }

            @Override
            public void deleteFile(final String fileName) {
                deletedPath = fileName;
                if (deleteException != null) {
                    throw deleteException;
                }
            }

            @Override
            public void close() {
            }
        }
    }

    private static TestRunner newRunner(final CapturingProviderService provider) throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new DeleteSmbExtended());
        runner.addControllerService("smb-provider", provider);
        runner.enableControllerService(provider);
        runner.setProperty(DeleteSmbExtended.SMB_CLIENT_PROVIDER_SERVICE, "smb-provider");
        return runner;
    }

    @Test
    public void testInputIsRequired() {
        final InputRequirement requirement = DeleteSmbExtended.class.getAnnotation(InputRequirement.class);
        assertNotNull(requirement, "@InputRequirement should be present on DeleteSmbExtended");
        assertEquals(InputRequirement.Requirement.INPUT_REQUIRED, requirement.value());
    }

    @Test
    public void testRemoteFileSupportsExpressionLanguage() {
        assertTrue(DeleteSmbExtended.REMOTE_FILE.isExpressionLanguageSupported(),
                "Remote File should support Expression Language");
    }

    @Test
    public void testSuccessfulDeletePublishesAttributesAndResolvesPath() throws Exception {
        final CapturingProviderService provider = new CapturingProviderService(null);
        final TestRunner runner = newRunner(provider);

        runner.enqueue("ignored", Map.of(
                "source_port", "4445",
                "path", "DATA/IN",
                "filename", "test.csv"));
        runner.run(1);

        runner.assertTransferCount(DeleteSmbExtended.REL_SUCCESS, 1);
        runner.assertTransferCount(DeleteSmbExtended.REL_NOT_FOUND, 0);
        runner.assertTransferCount(DeleteSmbExtended.REL_FAILURE, 0);

        assertEquals("DATA/IN/test.csv", provider.deletedPath,
                "Remote File EL should resolve from the incoming FlowFile attributes");
        assertNotNull(provider.capturedEvaluationAttributes);
        assertEquals("4445", provider.capturedEvaluationAttributes.get("source_port"));
    }

    @Test
    public void testSuccessfulDeleteAllowsBackslashRemotePath() throws Exception {
        final CapturingProviderService provider = new CapturingProviderService(null);
        final TestRunner runner = newRunner(provider);
        runner.setProperty(DeleteSmbExtended.REMOTE_FILE, "${source_directory}/${source_filename}");

        runner.enqueue("ignored", Map.of(
                "source_directory", "HOST\\SEWM0P002888.CON-02.EMEA.DC.CORPINTRA.NET\\DATA\\KRD\\TO-CRIS",
                "source_filename", "CSV_dubbeltest_20250819100002380.csv"));
        runner.run(1);

        runner.assertTransferCount(DeleteSmbExtended.REL_SUCCESS, 1);
        runner.assertTransferCount(DeleteSmbExtended.REL_NOT_FOUND, 0);
        runner.assertTransferCount(DeleteSmbExtended.REL_FAILURE, 0);

        assertEquals("HOST\\SEWM0P002888.CON-02.EMEA.DC.CORPINTRA.NET\\DATA\\KRD\\TO-CRIS/CSV_dubbeltest_20250819100002380.csv",
                provider.deletedPath,
                "The SMB delete call should receive the configured path unchanged; only provenance URI construction normalizes it");
    }

    @Test
    public void testMissingFileRoutesToNotFound() throws Exception {
        final CapturingProviderService provider = new CapturingProviderService(
                new SmbException("not found", DeleteSmbExtended.STATUS_OBJECT_NAME_NOT_FOUND, null));
        final TestRunner runner = newRunner(provider);

        runner.enqueue("ignored", Map.of("path", "DATA/IN", "filename", "missing.csv"));
        runner.run(1);

        runner.assertTransferCount(DeleteSmbExtended.REL_NOT_FOUND, 1);
        runner.assertTransferCount(DeleteSmbExtended.REL_SUCCESS, 0);
        runner.assertTransferCount(DeleteSmbExtended.REL_FAILURE, 0);
    }

    @Test
    public void testMissingPathRoutesToNotFound() throws Exception {
        final CapturingProviderService provider = new CapturingProviderService(
                new SmbException("path not found", DeleteSmbExtended.STATUS_OBJECT_PATH_NOT_FOUND, null));
        final TestRunner runner = newRunner(provider);

        runner.enqueue("ignored", Map.of("path", "DATA/NOPE", "filename", "x.csv"));
        runner.run(1);

        runner.assertTransferCount(DeleteSmbExtended.REL_NOT_FOUND, 1);
        runner.assertTransferCount(DeleteSmbExtended.REL_FAILURE, 0);
    }

    @Test
    public void testOtherErrorRoutesToFailureWithErrorAttributes() throws Exception {
        final CapturingProviderService provider = new CapturingProviderService(
                new SmbException("access denied", 0xC0000022L, null));
        final TestRunner runner = newRunner(provider);

        runner.enqueue("ignored", Map.of("path", "DATA/IN", "filename", "locked.csv"));
        runner.run(1);

        runner.assertTransferCount(DeleteSmbExtended.REL_FAILURE, 1);
        runner.assertTransferCount(DeleteSmbExtended.REL_SUCCESS, 0);
        runner.assertTransferCount(DeleteSmbExtended.REL_NOT_FOUND, 0);

        final MockFlowFile failure = runner.getFlowFilesForRelationship(DeleteSmbExtended.REL_FAILURE).get(0);
        failure.assertAttributeEquals(DeleteSmbExtended.ERROR_CODE_ATTRIBUTE, String.valueOf(0xC0000022L));
        failure.assertAttributeEquals(DeleteSmbExtended.ERROR_MESSAGE_ATTRIBUTE, "access denied");
    }

    @Test
    public void testEvaluationAttributesClearedAfterTrigger() throws Exception {
        final CapturingProviderService provider = new CapturingProviderService(null);
        final TestRunner runner = newRunner(provider);

        runner.enqueue("ignored", Map.of("path", "DATA/IN", "filename", "test.csv"));
        runner.run(1);

        final Field field = SmbjClientProviderServiceExtended.class.getDeclaredField("EVALUATION_ATTRIBUTES");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        final ThreadLocal<Map<String, String>> threadLocal = (ThreadLocal<Map<String, String>>) field.get(null);
        assertNull(threadLocal.get(),
                "Evaluation attributes must be cleared after onTrigger to avoid leaking onto other components");
    }
}
