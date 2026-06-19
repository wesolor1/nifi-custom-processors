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
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processors.azure.storage.DeleteAzureDataLakeStorage;
import org.apache.nifi.services.azure.storage.ADLSCredentialsControllerServiceExtended;

/**
 * Drop-in variant of {@link DeleteAzureDataLakeStorage} for use with {@link ADLSCredentialsControllerServiceExtended}
 * and {@link ListAzureDataLakeStorageExtended}. Processing logic is unchanged: incoming FlowFile attributes are
 * already passed to the configured ADLS Credentials service when resolving the storage account name.
 */
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"azure", "microsoft", "cloud", "storage", "adlsgen2", "datalake", "delete", "remove"})
@CapabilityDescription("Deletes a file or directory in Azure Data Lake Storage Gen 2. This is a drop-in variant of "
        + "DeleteAzureDataLakeStorage designed for use with ListAzureDataLakeStorageExtended and "
        + "ADLSCredentialsControllerServiceExtended, where the Storage Account Name can be supplied via Expression "
        + "Language against incoming FlowFile attributes (for example ${source_storage_account}). All other behavior "
        + "is identical to the standard DeleteAzureDataLakeStorage processor.")
@SeeAlso({DeleteAzureDataLakeStorage.class, ListAzureDataLakeStorageExtended.class,
        FetchAzureDataLakeStorageExtended.class, ADLSCredentialsControllerServiceExtended.class})
public class DeleteAzureDataLakeStorageExtended extends DeleteAzureDataLakeStorage {
}
