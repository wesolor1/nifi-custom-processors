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
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.file.transfer.FileTransfer;
import org.apache.nifi.processors.standard.DeleteSFTP;
import org.apache.nifi.processors.standard.FetchSFTP;
import org.apache.nifi.processors.standard.ListSFTP;
import org.apache.nifi.processors.standard.util.SFTPTransfer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@SupportsBatching(defaultDuration = DefaultRunDuration.TWENTY_FIVE_MILLIS)
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"remote", "remove", "delete", "sftp"})
@CapabilityDescription("Deletes a file residing on an SFTP server. This is a drop-in variant of DeleteSFTP where the Password property is NOT marked sensitive, "
        + "allowing the password to be supplied in plain text or via Expression Language referencing FlowFile attributes. When the resolved Password, "
        + "Private Key Path, or Private Key Passphrase is blank, it is treated as unset so the other credential can be used for dynamic password or "
        + "public-key authentication. When the remote file or directory does not exist, the FlowFile is routed to not found with an error.message "
        + "attribute naming the missing file and directory. When an existing file or directory cannot be deleted, the FlowFile is routed to failure "
        + "with an error.message attribute describing why the delete failed. All other behavior matches the standard DeleteSFTP processor.")
@SeeAlso({DeleteSFTP.class, ListSFTP.class, FetchSFTP.class})
@WritesAttributes({
        @WritesAttribute(attribute = DeleteSFTPExtended.ERROR_MESSAGE_ATTRIBUTE,
                description = "Why the delete did not succeed, including the remote path. Written when the FlowFile is routed to not found or failure.")
})
public class DeleteSFTPExtended extends DeleteSFTP {

    public static final String ERROR_MESSAGE_ATTRIBUTE = "error.message";

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

    /**
     * Same delete loop as {@link DeleteSFTP#onTrigger(ProcessContext, ProcessSession)}, with blank credentials
     * treated as unset. Not-found and failure routing also store {@link #ERROR_MESSAGE_ATTRIBUTE}; the stock processor only logs it.
     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ProcessContext effectiveContext = SftpBlankAsUnsetSupport.blankAsUnsetContext(context);
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final RemovalStrategy removalStrategy = effectiveContext.getProperty(REMOVAL_STRATEGY).asAllowableValue(RemovalStrategy.class);
        String hostname = effectiveContext.getProperty(FileTransfer.HOSTNAME).evaluateAttributeExpressions(flowFile).getValue();

        final int maxNumberOfFiles = effectiveContext.getProperty(SFTPTransfer.BATCH_SIZE).asInteger();
        int fileCount = 0;
        String directoryPathProperty = null;
        String filename = null;

        try (SFTPTransfer transfer = new SFTPTransfer(effectiveContext, logger)) {
            do {
                hostname = effectiveContext.getProperty(FileTransfer.HOSTNAME).evaluateAttributeExpressions(flowFile).getValue();
                final long startNanos = System.nanoTime();
                directoryPathProperty = effectiveContext.getProperty(DIRECTORY_PATH).evaluateAttributeExpressions(flowFile).getValue();
                filename = removalStrategy == RemovalStrategy.DIRECTORY
                        ? null
                        : effectiveContext.getProperty(FILENAME).evaluateAttributeExpressions(flowFile).getValue();

                try {
                    final Path directoryPath = Paths.get(directoryPathProperty).normalize();
                    final String transitUri;

                    if (removalStrategy == RemovalStrategy.DIRECTORY) {
                        transfer.deleteDirectory(flowFile, directoryPath.toString());
                        transitUri = "sftp://%s".formatted(directoryPath);
                    } else {
                        final Path filePath = directoryPath.resolve(filename).normalize();

                        if (!directoryPath.equals(filePath.getParent())) {
                            final String errorMessage = "Attempting to delete file at path '%s' which is not a direct child of the directory '%s'"
                                    .formatted(filePath, directoryPath);
                            handleFailure(session, flowFile, errorMessage, null);
                            continue;
                        }
                        transfer.deleteFile(flowFile, directoryPath.toString(), filename);
                        transitUri = "sftp://%s".formatted(filePath);
                    }

                    session.transfer(flowFile, REL_SUCCESS);
                    final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                    logger.debug("Successfully deleted file at path {} in {} millis; routing to success", flowFile, transferMillis);
                    session.getProvenanceReporter().invokeRemoteProcess(flowFile, transitUri, "Object deleted");
                } catch (final FileNotFoundException fileNotFoundException) {
                    final String errorMessage = formatErrorMessage(
                            notFoundMessage(filename, directoryPathProperty), fileNotFoundException);
                    logger.warn(errorMessage);
                    flowFile = session.putAttribute(flowFile, ERROR_MESSAGE_ATTRIBUTE, errorMessage);
                    session.transfer(flowFile, REL_NOT_FOUND);
                } catch (final IOException ioException) {
                    final String errorMessage;
                    if (removalStrategy == RemovalStrategy.DIRECTORY) {
                        errorMessage = "Failed to delete directory '%s'".formatted(directoryPathProperty);
                    } else {
                        errorMessage = "Failed to delete file '%s' in directory '%s'".formatted(filename, directoryPathProperty);
                    }
                    handleFailure(session, flowFile, errorMessage, ioException);
                }
            } while (isScheduled()
                    && (getRelationships().size() == effectiveContext.getAvailableRelationships().size())
                    && (++fileCount < maxNumberOfFiles)
                    && ((flowFile = session.get()) != null));
        } catch (final IOException | FlowFileAccessException | ProcessException e) {
            effectiveContext.yield();

            final Throwable cause = switch (e) {
                case FlowFileAccessException flowFileAccessException ->
                        flowFileAccessException.getCause() != null ? flowFileAccessException.getCause() : flowFileAccessException;
                case ProcessException processException ->
                        processException.getCause() != null ? processException.getCause() : processException;
                default -> e;
            };

            final String target = filename != null
                    ? "file '%s' in directory '%s'".formatted(filename, directoryPathProperty)
                    : directoryPathProperty != null
                    ? "directory '%s'".formatted(directoryPathProperty)
                    : String.valueOf(flowFile);
            final String errorMessage = "Unable to delete %s from remote host %s".formatted(target, hostname);
            handleFailure(session, flowFile, errorMessage, cause);
        }
    }

    private void handleFailure(final ProcessSession session, FlowFile flowFile, final String summary, final Throwable throwable) {
        final String errorMessage = formatErrorMessage(summary, throwable);
        if (throwable == null) {
            getLogger().error(errorMessage);
        } else {
            getLogger().error(errorMessage, throwable);
        }

        flowFile = session.putAttribute(flowFile, ERROR_MESSAGE_ATTRIBUTE, errorMessage);
        session.penalize(flowFile);
        session.transfer(flowFile, REL_FAILURE);
    }

    static String notFoundMessage(final String filename, final String directory) {
        if (filename == null) {
            return "Directory '%s' not found".formatted(directory);
        }
        return "Filename '%s' not found in directory '%s'".formatted(filename, directory);
    }

    static String formatErrorMessage(final String summary, final Throwable throwable) {
        if (throwable == null) {
            return summary;
        }
        final String detail = throwableDetail(throwable);
        if (detail.isEmpty()) {
            return summary + ": " + throwable.getClass().getSimpleName();
        }
        if (summary.endsWith(detail)) {
            return summary;
        }
        return summary + ": " + detail;
    }

    private static String throwableDetail(final Throwable throwable) {
        final StringBuilder detail = new StringBuilder();
        Throwable current = throwable;
        int guard = 0;
        while (current != null && guard++ < 10) {
            appendMessage(detail, current.getMessage());
            final Throwable cause = current.getCause();
            if (cause == current) {
                break;
            }
            current = cause;
        }
        return detail.toString();
    }

    private static void appendMessage(final StringBuilder detail, final String message) {
        if (message == null || message.isBlank()) {
            return;
        }
        final String trimmed = message.trim();
        if (detail.indexOf(trimmed) >= 0) {
            return;
        }
        if (detail.length() > 0) {
            detail.append(": ");
        }
        detail.append(trimmed);
    }
}
