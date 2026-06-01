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
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processors.standard.FetchSFTP;
import org.apache.nifi.processors.standard.GetFTP;
import org.apache.nifi.processors.standard.GetSFTP;
import org.apache.nifi.processors.standard.PutFTP;
import org.apache.nifi.processors.standard.PutSFTP;
import org.apache.nifi.processors.standard.util.SFTPTransfer;

import java.util.ArrayList;
import java.util.List;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"sftp", "get", "retrieve", "files", "fetch", "remote", "ingest", "source", "input"})
@CapabilityDescription("Fetches the content of a file from a remote SFTP server and overwrites the contents of an incoming FlowFile with the content of the remote file. "
        + "This is a drop-in variant of FetchSFTP where the Password property is NOT marked sensitive, allowing the password to be supplied in plain text "
        + "or via Expression Language referencing FlowFile attributes. All other behavior is identical to the standard FetchSFTP processor.")
@SeeAlso({FetchSFTP.class, GetSFTP.class, PutSFTP.class, GetFTP.class, PutFTP.class})
@WritesAttributes({
        @WritesAttribute(attribute = "sftp.remote.host", description = "The hostname or IP address from which the file was pulled"),
        @WritesAttribute(attribute = "sftp.remote.port", description = "The port that was used to communicate with the remote SFTP server"),
        @WritesAttribute(attribute = "sftp.remote.filename", description = "The name of the remote file that was pulled"),
        @WritesAttribute(attribute = "filename", description = "The filename is updated to point to the filename fo the remote file"),
        @WritesAttribute(attribute = "path", description = "If the Remote File contains a directory name, that directory name will be added to the FlowFile using the 'path' attribute"),
        @WritesAttribute(attribute = "fetch.failure.reason", description = "The name of the failure relationship applied when routing to any failure relationship")
})
public class FetchSFTPExtended extends FetchSFTP {

    /**
     * Non-sensitive variant of the stock {@link SFTPTransfer#PASSWORD} descriptor. It keeps the same name
     * ("Password") and every other setting (including Expression Language scope) so the configured value still
     * resolves when {@code SFTPTransfer} reads {@code context.getProperty(SFTPTransfer.PASSWORD)}; only the
     * sensitivity flag is flipped to {@code false}.
     */
    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SFTPTransfer.PASSWORD)
            .sensitive(false)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> baseDescriptors = super.getSupportedPropertyDescriptors();
        final List<PropertyDescriptor> descriptors = new ArrayList<>(baseDescriptors.size());
        for (final PropertyDescriptor descriptor : baseDescriptors) {
            if (descriptor.getName().equals(SFTPTransfer.PASSWORD.getName())) {
                descriptors.add(PASSWORD);
            } else {
                descriptors.add(descriptor);
            }
        }
        return descriptors;
    }
}
