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

import org.apache.nifi.annotation.behavior.DefaultRunDuration;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.standard.DeleteSFTP;
import org.apache.nifi.processors.standard.FetchSFTP;
import org.apache.nifi.processors.standard.ListSFTP;
import org.apache.nifi.processors.standard.util.SFTPTransfer;

import java.util.ArrayList;
import java.util.List;

@SupportsBatching(defaultDuration = DefaultRunDuration.TWENTY_FIVE_MILLIS)
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"remote", "remove", "delete", "sftp"})
@CapabilityDescription("Deletes a file residing on an SFTP server. This is a drop-in variant of DeleteSFTP where the Password property is NOT marked sensitive, "
        + "allowing the password to be supplied in plain text or via Expression Language referencing FlowFile attributes. When the resolved Password, "
        + "Private Key Path, or Private Key Passphrase is blank, it is treated as unset so the other credential can be used for dynamic password or "
        + "public-key authentication. All other behavior is identical to the standard DeleteSFTP processor.")
@SeeAlso({DeleteSFTP.class, ListSFTP.class, FetchSFTP.class})
public class DeleteSFTPExtended extends DeleteSFTP {

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

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        super.onTrigger(SftpBlankAsUnsetSupport.blankAsUnsetContext(context), session);
    }
}
