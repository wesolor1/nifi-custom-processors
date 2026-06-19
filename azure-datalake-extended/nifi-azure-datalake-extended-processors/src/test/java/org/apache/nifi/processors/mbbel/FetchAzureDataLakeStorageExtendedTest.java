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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processors.azure.storage.FetchAzureDataLakeStorage;
import org.apache.nifi.services.azure.storage.ADLSCredentialsControllerServiceExtended;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FetchAzureDataLakeStorageExtendedTest {

    @Test
    public void testInputRequired() {
        final InputRequirement requirement = FetchAzureDataLakeStorageExtended.class.getAnnotation(InputRequirement.class);
        assertNotNull(requirement);
        assertEquals(Requirement.INPUT_REQUIRED, requirement.value());
    }

    @Test
    public void testPropertiesMatchStockFetchAzureDataLakeStorage() {
        final List<PropertyDescriptor> base = new FetchAzureDataLakeStorage().getPropertyDescriptors();
        final List<PropertyDescriptor> extended = new FetchAzureDataLakeStorageExtended().getPropertyDescriptors();

        assertEquals(base.size(), extended.size());
        for (int i = 0; i < base.size(); i++) {
            assertEquals(base.get(i), extended.get(i));
        }
    }

    @Test
    public void testSeeAlsoIncludesExtendedCredentialsService() {
        final SeeAlso seeAlso = FetchAzureDataLakeStorageExtended.class.getAnnotation(SeeAlso.class);
        assertNotNull(seeAlso);
        assertTrue(Arrays.asList(seeAlso.value()).contains(ADLSCredentialsControllerServiceExtended.class));
        assertTrue(Arrays.asList(seeAlso.value()).contains(ListAzureDataLakeStorageExtended.class));
    }
}
