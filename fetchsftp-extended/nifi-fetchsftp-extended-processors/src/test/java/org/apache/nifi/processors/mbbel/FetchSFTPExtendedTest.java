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
import org.apache.nifi.processors.standard.FetchSFTP;
import org.apache.nifi.processors.standard.util.SFTPTransfer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FetchSFTPExtendedTest {

    private PropertyDescriptor passwordDescriptor(final FetchSFTPExtended processor) {
        return processor.getPropertyDescriptors().stream()
                .filter(pd -> pd.getName().equals(SFTPTransfer.PASSWORD.getName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Password descriptor not exposed by processor"));
    }

    @Test
    public void testPasswordIsNotSensitive() {
        assertFalse(FetchSFTPExtended.PASSWORD.isSensitive(),
                "FetchSFTPExtended PASSWORD descriptor should override stock SFTPTransfer.PASSWORD to be non-sensitive");
        assertFalse(passwordDescriptor(new FetchSFTPExtended()).isSensitive(),
                "The Password descriptor actually exposed by the processor should be non-sensitive");
    }

    @Test
    public void testPasswordBehaviorUnchangedApartFromSensitivity() {
        // Same name -> the value still resolves when SFTPTransfer reads context.getProperty(SFTPTransfer.PASSWORD).
        assertEquals(SFTPTransfer.PASSWORD.getName(), FetchSFTPExtended.PASSWORD.getName());
        // Expression Language scope must be preserved so dynamic passwords keep working.
        assertEquals(SFTPTransfer.PASSWORD.getExpressionLanguageScope(),
                FetchSFTPExtended.PASSWORD.getExpressionLanguageScope());
        assertEquals(SFTPTransfer.PASSWORD.isRequired(), FetchSFTPExtended.PASSWORD.isRequired());
    }

    @Test
    public void testOnlyPasswordDiffersFromStockFetchSFTP() {
        final List<PropertyDescriptor> base = new FetchSFTP().getPropertyDescriptors();
        final List<PropertyDescriptor> extended = new FetchSFTPExtended().getPropertyDescriptors();

        assertEquals(base.size(), extended.size(), "Extended processor must expose the same set of properties");
        for (int i = 0; i < base.size(); i++) {
            final PropertyDescriptor baseDescriptor = base.get(i);
            final PropertyDescriptor extendedDescriptor = extended.get(i);
            assertEquals(baseDescriptor.getName(), extendedDescriptor.getName(),
                    "Property ordering and names must be preserved");
            if (baseDescriptor.getName().equals(SFTPTransfer.PASSWORD.getName())) {
                assertTrue(baseDescriptor.isSensitive(), "Stock FetchSFTP Password is sensitive");
                assertFalse(extendedDescriptor.isSensitive(), "Extended Password must be non-sensitive");
            } else {
                assertEquals(baseDescriptor.isSensitive(), extendedDescriptor.isSensitive(),
                        "Sensitivity of non-password properties must be unchanged: " + baseDescriptor.getName());
            }
        }
    }

    @Test
    public void testInputRequirementMatchesStockFetchSFTP() {
        final InputRequirement requirement = FetchSFTPExtended.class.getAnnotation(InputRequirement.class);
        assertNotNull(requirement, "@InputRequirement should be present on FetchSFTPExtended");
        assertEquals(Requirement.INPUT_REQUIRED, requirement.value());
    }

    @Test
    public void testRelationshipsAreInheritedFromFetchSFTP() {
        final FetchSFTPExtended extended = new FetchSFTPExtended();
        final Optional<String> missing = new FetchSFTP().getRelationships().stream()
                .map(r -> r.getName())
                .filter(name -> extended.getRelationships().stream().noneMatch(r -> r.getName().equals(name)))
                .findFirst();
        assertTrue(missing.isEmpty(), "Extended processor should expose all stock FetchSFTP relationships; missing: " + missing.orElse(""));
    }
}
