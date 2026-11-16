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
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.standard.DeleteSFTP;
import org.apache.nifi.processors.standard.util.SFTPTransfer;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DeleteSFTPExtendedTest {

    private PropertyDescriptor passwordDescriptor(final DeleteSFTPExtended processor) {
        return processor.getPropertyDescriptors().stream()
                .filter(pd -> pd.getName().equals(SFTPTransfer.PASSWORD.getName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Password descriptor not exposed by processor"));
    }

    @Test
    public void testPasswordIsNotSensitive() {
        assertFalse(DeleteSFTPExtended.PASSWORD.isSensitive(),
                "DeleteSFTPExtended PASSWORD descriptor should override stock SFTPTransfer.PASSWORD to be non-sensitive");
        assertFalse(passwordDescriptor(new DeleteSFTPExtended()).isSensitive(),
                "The Password descriptor actually exposed by the processor should be non-sensitive");
    }

    @Test
    public void testPasswordBehaviorUnchangedApartFromSensitivity() {
        assertEquals(SFTPTransfer.PASSWORD.getName(), DeleteSFTPExtended.PASSWORD.getName());
        assertEquals(SFTPTransfer.PASSWORD.getExpressionLanguageScope(),
                DeleteSFTPExtended.PASSWORD.getExpressionLanguageScope());
        assertEquals(SFTPTransfer.PASSWORD.isRequired(), DeleteSFTPExtended.PASSWORD.isRequired());
    }

    @Test
    public void testOnlyPasswordDiffersFromStockDeleteSFTP() {
        final List<PropertyDescriptor> base = new DeleteSFTP().getPropertyDescriptors();
        final List<PropertyDescriptor> extended = new DeleteSFTPExtended().getPropertyDescriptors();

        assertEquals(base.size(), extended.size(), "Extended processor must expose the same set of properties");
        for (int i = 0; i < base.size(); i++) {
            final PropertyDescriptor baseDescriptor = base.get(i);
            final PropertyDescriptor extendedDescriptor = extended.get(i);
            assertEquals(baseDescriptor.getName(), extendedDescriptor.getName(),
                    "Property ordering and names must be preserved");
            if (baseDescriptor.getName().equals(SFTPTransfer.PASSWORD.getName())) {
                assertTrue(baseDescriptor.isSensitive(), "Stock DeleteSFTP Password is sensitive");
                assertFalse(extendedDescriptor.isSensitive(), "Extended Password must be non-sensitive");
            } else {
                assertEquals(baseDescriptor.isSensitive(), extendedDescriptor.isSensitive(),
                        "Sensitivity of non-password properties must be unchanged: " + baseDescriptor.getName());
            }
        }
    }

    @Test
    public void testInputRequirementMatchesStockDeleteSFTP() {
        final InputRequirement requirement = DeleteSFTPExtended.class.getAnnotation(InputRequirement.class);
        assertNotNull(requirement, "@InputRequirement should be present on DeleteSFTPExtended");
        assertEquals(Requirement.INPUT_REQUIRED, requirement.value());
    }

    @Test
    public void testRelationshipsAreInheritedFromDeleteSFTP() {
        final DeleteSFTPExtended extended = new DeleteSFTPExtended();
        final Optional<String> missing = new DeleteSFTP().getRelationships().stream()
                .map(r -> r.getName())
                .filter(name -> extended.getRelationships().stream().noneMatch(r -> r.getName().equals(name)))
                .findFirst();
        assertTrue(missing.isEmpty(),
                "Extended processor should expose all stock DeleteSFTP relationships; missing: " + missing.orElse(""));
    }

    @Test
    public void testBlankPasswordResolvesToNullSoKeyAuthCanBeUsed() {
        final TestRunner runner = TestRunners.newTestRunner(new DeleteSFTPExtended());
        runner.setProperty(SFTPTransfer.HOSTNAME, "host");
        runner.setProperty(SFTPTransfer.PORT, "22");
        runner.setProperty(SFTPTransfer.USERNAME, "user");
        runner.setProperty(DeleteSFTPExtended.PASSWORD, "${target_password}");
        runner.setProperty(SFTPTransfer.PRIVATE_KEY_PATH, "${target_private_key_path}");

        final ProcessContext sanitized = SftpBlankAsUnsetSupport.blankAsUnsetContext(runner.getProcessContext());

        assertNull(sanitized.getProperty(SFTPTransfer.PASSWORD)
                .evaluateAttributeExpressions(Map.of("target_private_key_path", "/opt/nifi/keys/key.pem")).getValue());
        assertEquals("/opt/nifi/keys/key.pem", sanitized.getProperty(SFTPTransfer.PRIVATE_KEY_PATH)
                .evaluateAttributeExpressions(Map.of("target_private_key_path", "/opt/nifi/keys/key.pem")).getValue());
    }

    @Test
    public void testBlankPrivateKeyResolvesToNullSoPasswordAuthCanBeUsed() {
        final TestRunner runner = TestRunners.newTestRunner(new DeleteSFTPExtended());
        runner.setProperty(SFTPTransfer.HOSTNAME, "host");
        runner.setProperty(SFTPTransfer.PORT, "22");
        runner.setProperty(SFTPTransfer.USERNAME, "user");
        runner.setProperty(DeleteSFTPExtended.PASSWORD, "${target_password}");
        runner.setProperty(SFTPTransfer.PRIVATE_KEY_PATH, "${target_private_key_path}");

        final ProcessContext sanitized = SftpBlankAsUnsetSupport.blankAsUnsetContext(runner.getProcessContext());
        final Map<String, String> attributes = Map.of("target_password", "secret");

        assertEquals("secret", sanitized.getProperty(SFTPTransfer.PASSWORD)
                .evaluateAttributeExpressions(attributes).getValue());
        assertNull(sanitized.getProperty(SFTPTransfer.PRIVATE_KEY_PATH)
                .evaluateAttributeExpressions(attributes).getValue());
    }

    @Test
    public void testNotFoundMessageNamesFileAndDirectory() {
        assertEquals(
                "Filename 'example.txt' not found in directory '/incoming'",
                DeleteSFTPExtended.notFoundMessage("example.txt", "/incoming"));
    }

    @Test
    public void testNotFoundMessageNamesDirectoryWhenRemovingADirectory() {
        assertEquals(
                "Directory '/incoming' not found",
                DeleteSFTPExtended.notFoundMessage(null, "/incoming"));
    }

    @Test
    public void testNotFoundMessageKeepsUnderlyingSftpDetail() {
        final FileNotFoundException missing = new FileNotFoundException("No such file");

        assertEquals(
                "Filename 'example.txt' not found in directory '/incoming': No such file",
                DeleteSFTPExtended.formatErrorMessage(
                        DeleteSFTPExtended.notFoundMessage("example.txt", "/incoming"), missing));
    }

    @Test
    public void testErrorMessageIncludesCauseChain() {
        final IOException failure = new IOException("Failed to delete [/incoming/report.csv]",
                new IOException("3: Permission denied"));

        assertEquals(
                "Failed to delete file 'report.csv' in directory '/incoming': "
                        + "Failed to delete [/incoming/report.csv]: 3: Permission denied",
                DeleteSFTPExtended.formatErrorMessage(
                        "Failed to delete file 'report.csv' in directory '/incoming'", failure));
    }

    @Test
    public void testFailureSetsErrorMessageWhenFilenameIsNotInDirectory() {
        final TestRunner runner = TestRunners.newTestRunner(new DeleteSFTPExtended());
        runner.setProperty(SFTPTransfer.HOSTNAME, "127.0.0.1");
        runner.setProperty(SFTPTransfer.USERNAME, "user");
        runner.setProperty(DeleteSFTPExtended.PASSWORD, "secret");
        runner.setProperty(DeleteSFTP.DIRECTORY_PATH, "/incoming");
        runner.setProperty(DeleteSFTP.FILENAME, "nested/report.csv");

        runner.enqueue("payload");
        runner.run();

        runner.assertTransferCount(DeleteSFTP.REL_FAILURE, 1);
        runner.assertTransferCount(DeleteSFTP.REL_SUCCESS, 0);
        runner.assertTransferCount(DeleteSFTP.REL_NOT_FOUND, 0);
        final MockFlowFile failure = runner.getFlowFilesForRelationship(DeleteSFTP.REL_FAILURE).get(0);
        final String errorMessage = failure.getAttribute(DeleteSFTPExtended.ERROR_MESSAGE_ATTRIBUTE);
        assertNotNull(errorMessage);
        assertTrue(errorMessage.contains("report.csv"), errorMessage);
        assertTrue(errorMessage.contains("not a direct child"), errorMessage);
    }

    @Test
    public void testFailureSetsErrorMessageWhenDeleteCannotConnect() throws IOException {
        final int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }

        final TestRunner runner = TestRunners.newTestRunner(new DeleteSFTPExtended());
        runner.setProperty(SFTPTransfer.HOSTNAME, "127.0.0.1");
        runner.setProperty(SFTPTransfer.PORT, Integer.toString(closedPort));
        runner.setProperty(SFTPTransfer.USERNAME, "user");
        runner.setProperty(DeleteSFTPExtended.PASSWORD, "secret");
        runner.setProperty(SFTPTransfer.CONNECTION_TIMEOUT, "3 sec");
        runner.setProperty(SFTPTransfer.DATA_TIMEOUT, "3 sec");
        runner.setProperty(DeleteSFTP.DIRECTORY_PATH, "/incoming");
        runner.setProperty(DeleteSFTP.FILENAME, "report.csv");

        runner.enqueue("payload");
        runner.run();

        runner.assertTransferCount(DeleteSFTP.REL_FAILURE, 1);
        runner.assertTransferCount(DeleteSFTP.REL_SUCCESS, 0);
        final MockFlowFile failure = runner.getFlowFilesForRelationship(DeleteSFTP.REL_FAILURE).get(0);
        final String errorMessage = failure.getAttribute(DeleteSFTPExtended.ERROR_MESSAGE_ATTRIBUTE);
        assertNotNull(errorMessage);
        assertTrue(errorMessage.contains("report.csv"), errorMessage);
        assertTrue(errorMessage.contains("/incoming"), errorMessage);
        assertTrue(errorMessage.toLowerCase().contains("connection refused"), errorMessage);
    }
}
