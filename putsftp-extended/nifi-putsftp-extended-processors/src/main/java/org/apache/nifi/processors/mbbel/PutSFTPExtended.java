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
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processors.standard.FetchSFTP;
import org.apache.nifi.processors.standard.GetSFTP;
import org.apache.nifi.processors.standard.PutSFTP;
import org.apache.nifi.processors.standard.util.SFTPTransfer;

import java.util.ArrayList;
import java.util.List;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"remote", "copy", "egress", "put", "sftp", "archive", "files"})
@CapabilityDescription("Sends FlowFiles to an SFTP Server. This is a drop-in variant of PutSFTP where the Password property is NOT marked sensitive, "
        + "allowing the password to be supplied in plain text or via Expression Language referencing FlowFile attributes. "
        + "All other behavior is identical to the standard PutSFTP processor.")
@SeeAlso({PutSFTP.class, GetSFTP.class, FetchSFTP.class})
public class PutSFTPExtended extends PutSFTP {

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
