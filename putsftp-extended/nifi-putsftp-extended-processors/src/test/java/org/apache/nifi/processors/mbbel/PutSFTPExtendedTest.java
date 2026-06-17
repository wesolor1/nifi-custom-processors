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
import org.apache.nifi.processors.standard.PutSFTP;
import org.apache.nifi.processors.standard.util.SFTPTransfer;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PutSFTPExtendedTest {

    private PropertyDescriptor passwordDescriptor(final PutSFTPExtended processor) {
        return processor.getPropertyDescriptors().stream()
                .filter(pd -> pd.getName().equals(SFTPTransfer.PASSWORD.getName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Password descriptor not exposed by processor"));
    }

    @Test
    public void testPasswordIsNotSensitive() {
        assertFalse(PutSFTPExtended.PASSWORD.isSensitive(),
                "PutSFTPExtended PASSWORD descriptor should override stock SFTPTransfer.PASSWORD to be non-sensitive");
        assertFalse(passwordDescriptor(new PutSFTPExtended()).isSensitive(),
                "The Password descriptor actually exposed by the processor should be non-sensitive");
    }

    @Test
    public void testPasswordBehaviorUnchangedApartFromSensitivity() {
        // Same name -> the value still resolves when SFTPTransfer reads context.getProperty(SFTPTransfer.PASSWORD).
        assertEquals(SFTPTransfer.PASSWORD.getName(), PutSFTPExtended.PASSWORD.getName());
        // Expression Language scope must be preserved so dynamic passwords keep working.
        assertEquals(SFTPTransfer.PASSWORD.getExpressionLanguageScope(),
                PutSFTPExtended.PASSWORD.getExpressionLanguageScope());
        assertEquals(SFTPTransfer.PASSWORD.isRequired(), PutSFTPExtended.PASSWORD.isRequired());
    }

    @Test
    public void testOnlyPasswordDiffersFromStockPutSFTP() {
        final List<PropertyDescriptor> base = new PutSFTP().getPropertyDescriptors();
        final List<PropertyDescriptor> extended = new PutSFTPExtended().getPropertyDescriptors();

        assertEquals(base.size(), extended.size(), "Extended processor must expose the same set of properties");
        for (int i = 0; i < base.size(); i++) {
            final PropertyDescriptor baseDescriptor = base.get(i);
            final PropertyDescriptor extendedDescriptor = extended.get(i);
            assertEquals(baseDescriptor.getName(), extendedDescriptor.getName(),
                    "Property ordering and names must be preserved");
            if (baseDescriptor.getName().equals(SFTPTransfer.PASSWORD.getName())) {
                assertTrue(baseDescriptor.isSensitive(), "Stock PutSFTP Password is sensitive");
                assertFalse(extendedDescriptor.isSensitive(), "Extended Password must be non-sensitive");
            } else {
                assertEquals(baseDescriptor.isSensitive(), extendedDescriptor.isSensitive(),
                        "Sensitivity of non-password properties must be unchanged: " + baseDescriptor.getName());
            }
        }
    }

    @Test
    public void testInputRequirementMatchesStockPutSFTP() {
        final InputRequirement requirement = PutSFTPExtended.class.getAnnotation(InputRequirement.class);
        assertNotNull(requirement, "@InputRequirement should be present on PutSFTPExtended");
        assertEquals(Requirement.INPUT_REQUIRED, requirement.value());
    }

    @Test
    public void testBlankPasswordResolvesToNullSoKeyAuthCanBeUsed() {
        final TestRunner runner = TestRunners.newTestRunner(new PutSFTPExtended());
        runner.setProperty(SFTPTransfer.HOSTNAME, "host");
        runner.setProperty(SFTPTransfer.PORT, "22");
        runner.setProperty(SFTPTransfer.USERNAME, "user");
        runner.setProperty(PutSFTPExtended.PASSWORD, "${target_password}");
        runner.setProperty(SFTPTransfer.PRIVATE_KEY_PATH, "${target_private_key_path}");

        final ProcessContext sanitized = SftpBlankAsUnsetSupport.blankAsUnsetContext(runner.getProcessContext());

        // Empty attribute -> blank EL result -> treated as unset (null), matching StandardSshClientProvider's != null check.
        final String password = sanitized.getProperty(SFTPTransfer.PASSWORD)
                .evaluateAttributeExpressions(Map.of("target_private_key_path", "/opt/nifi/keys/key.pem")).getValue();
        assertNull(password, "Blank password must resolve to null so no empty password identity is added");

        final String privateKeyPath = sanitized.getProperty(SFTPTransfer.PRIVATE_KEY_PATH)
                .evaluateAttributeExpressions(Map.of("target_private_key_path", "/opt/nifi/keys/key.pem")).getValue();
        assertEquals("/opt/nifi/keys/key.pem", privateKeyPath, "Populated private key path must be preserved");
    }

    @Test
    public void testBlankPrivateKeyResolvesToNullSoPasswordAuthCanBeUsed() {
        final TestRunner runner = TestRunners.newTestRunner(new PutSFTPExtended());
        runner.setProperty(SFTPTransfer.HOSTNAME, "host");
        runner.setProperty(SFTPTransfer.PORT, "22");
        runner.setProperty(SFTPTransfer.USERNAME, "user");
        runner.setProperty(PutSFTPExtended.PASSWORD, "${target_password}");
        runner.setProperty(SFTPTransfer.PRIVATE_KEY_PATH, "${target_private_key_path}");

        final ProcessContext sanitized = SftpBlankAsUnsetSupport.blankAsUnsetContext(runner.getProcessContext());
        final Map<String, String> attributes = Map.of("target_password", "secret");

        final String password = sanitized.getProperty(SFTPTransfer.PASSWORD)
                .evaluateAttributeExpressions(attributes).getValue();
        assertEquals("secret", password, "Populated password must be preserved");

        final String privateKeyPath = sanitized.getProperty(SFTPTransfer.PRIVATE_KEY_PATH)
                .evaluateAttributeExpressions(attributes).getValue();
        assertNull(privateKeyPath, "Blank private key path must resolve to null so no empty key provider is configured");
    }

    @Test
    public void testNonAuthPropertyIsNotAlteredByBlankSanitization() {
        final TestRunner runner = TestRunners.newTestRunner(new PutSFTPExtended());
        runner.setProperty(SFTPTransfer.HOSTNAME, "host");
        runner.setProperty(SFTPTransfer.PORT, "22");
        runner.setProperty(SFTPTransfer.USERNAME, "user");

        final ProcessContext sanitized = SftpBlankAsUnsetSupport.blankAsUnsetContext(runner.getProcessContext());
        assertEquals("host", sanitized.getProperty(SFTPTransfer.HOSTNAME).evaluateAttributeExpressions(Map.of()).getValue());
        assertEquals("user", sanitized.getProperty(SFTPTransfer.USERNAME).evaluateAttributeExpressions(Map.of()).getValue());
    }

    @Test
    public void testRelationshipsAreInheritedFromPutSFTP() {
        final PutSFTPExtended extended = new PutSFTPExtended();
        final Optional<String> missing = new PutSFTP().getRelationships().stream()
                .map(r -> r.getName())
                .filter(name -> extended.getRelationships().stream().noneMatch(r -> r.getName().equals(name)))
                .findFirst();
        assertTrue(missing.isEmpty(), "Extended processor should expose all stock PutSFTP relationships; missing: " + missing.orElse(""));
    }
}
