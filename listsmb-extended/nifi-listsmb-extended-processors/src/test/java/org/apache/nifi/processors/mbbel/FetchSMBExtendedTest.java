package org.apache.nifi.processors.mbbel;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.smb.FetchSmb;
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
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FetchSMBExtendedTest {

    /**
     * Captures the value of {@link SmbjClientProviderServiceExtended}'s static evaluation-attributes
     * ThreadLocal at the moment {@code getClient} is invoked, so we can verify the processor published
     * the trigger FlowFile's attributes to the extended provider before opening a connection.
     */
    private static class CapturingProviderService extends AbstractControllerService implements SmbClientProviderService {

        private volatile Map<String, String> capturedEvaluationAttributes;

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
            return new NoOpClient();
        }
    }

    private static class NoOpClient implements SmbClientService {
        @Override
        public Stream<SmbListableEntity> listFiles(final String directory) {
            return Stream.empty();
        }

        @Override
        public void ensureDirectory(final String directory) {
        }

        @Override
        public void readFile(final String fileName, final OutputStream outputStream) throws IOException {
        }

        @Override
        public void moveFile(final String fileName, final String targetDirectory) {
        }

        @Override
        public void deleteFile(final String fileName) {
        }

        @Override
        public void close() {
        }
    }

    private static TestRunner newRunner(final CapturingProviderService provider) throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new FetchSMBExtended());
        runner.addControllerService("smb-provider", provider);
        runner.enableControllerService(provider);
        runner.setProperty(FetchSmb.SMB_CLIENT_PROVIDER_SERVICE, "smb-provider");
        return runner;
    }

    @Test
    public void testInputIsRequired() {
        final InputRequirement requirement = FetchSMBExtended.class.getAnnotation(InputRequirement.class);
        assertNotNull(requirement, "@InputRequirement should be present on FetchSMBExtended");
        assertEquals(InputRequirement.Requirement.INPUT_REQUIRED, requirement.value());
    }

    @Test
    public void testFetchPublishesIncomingAttributesToProviderService() throws Exception {
        final CapturingProviderService provider = new CapturingProviderService();
        final TestRunner runner = newRunner(provider);

        runner.enqueue("ignored", Map.of(
                "source_host", "files.example.com",
                "source_port", "4445",
                "path", "DATA/IN",
                "filename", "test.csv"));
        runner.run(1);

        runner.assertTransferCount(FetchSmb.REL_SUCCESS, 1);
        runner.assertTransferCount(FetchSmb.REL_FAILURE, 0);

        assertNotNull(provider.capturedEvaluationAttributes,
                "Provider should have seen trigger attributes via the evaluation-attributes ThreadLocal");
        assertEquals("4445", provider.capturedEvaluationAttributes.get("source_port"));
        assertEquals("files.example.com", provider.capturedEvaluationAttributes.get("source_host"));
    }

    @Test
    public void testEvaluationAttributesClearedAfterTrigger() throws Exception {
        final CapturingProviderService provider = new CapturingProviderService();
        final TestRunner runner = newRunner(provider);

        runner.enqueue("ignored", Map.of("source_port", "4445", "path", "DATA/IN", "filename", "test.csv"));
        runner.run(1);

        final Field field = SmbjClientProviderServiceExtended.class.getDeclaredField("EVALUATION_ATTRIBUTES");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        final ThreadLocal<Map<String, String>> threadLocal = (ThreadLocal<Map<String, String>>) field.get(null);
        assertEquals(null, threadLocal.get(),
                "Evaluation attributes must be cleared after onTrigger to avoid leaking onto other components");
    }

    @Test
    public void testRemoteFileSupportsExpressionLanguage() {
        assertTrue(FetchSmb.REMOTE_FILE.isExpressionLanguageSupported(),
                "Remote File should support Expression Language (inherited from FetchSmb)");
    }
}
