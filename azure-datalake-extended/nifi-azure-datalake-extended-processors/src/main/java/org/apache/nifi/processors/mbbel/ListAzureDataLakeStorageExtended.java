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

import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.list.ListedEntityTracker;
import org.apache.nifi.processors.azure.storage.AbstractListAzureProcessor;
import org.apache.nifi.processors.azure.storage.ListAzureDataLakeStorage;
import org.apache.nifi.processors.azure.storage.utils.ADLSFileInfo;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.processors.azure.storage.utils.DataLakeServiceClientFactory;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.services.azure.storage.ADLSCredentialsDetails;
import org.apache.nifi.services.azure.storage.ADLSCredentialsService;
import org.apache.nifi.services.azure.storage.ADLSCredentialsControllerServiceExtended;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.nifi.processor.util.list.ListedEntityTracker.INITIAL_LISTING_TARGET;
import static org.apache.nifi.processor.util.list.ListedEntityTracker.TRACKING_STATE_CACHE;
import static org.apache.nifi.processor.util.list.ListedEntityTracker.TRACKING_TIME_WINDOW;
import static org.apache.nifi.processors.azure.AbstractAzureDataLakeStorageProcessor.TEMP_FILE_DIRECTORY;
import static org.apache.nifi.processors.azure.storage.ListAzureDataLakeStorage.INCLUDE_TEMPORARY_FILES;
import static org.apache.nifi.processors.azure.storage.ListAzureDataLakeStorage.RECURSE_SUBDIRECTORIES;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_DIRECTORY;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_ETAG;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_FILENAME;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_FILESYSTEM;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_FILE_PATH;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_LAST_MODIFIED;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_LENGTH;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_DIRECTORY;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_ETAG;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_FILENAME;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_FILESYSTEM;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_FILE_PATH;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_LAST_MODIFIED;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_LENGTH;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.ADLS_CREDENTIALS_SERVICE;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.DIRECTORY;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.FILESYSTEM;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.evaluateDirectoryProperty;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.evaluateFileSystemProperty;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.getProxyOptions;

/**
 * Extended variant of {@link ListAzureDataLakeStorage} that accepts incoming connections. It is implemented as a
 * sibling (extending {@link AbstractListAzureProcessor} directly) rather than a subclass of the stock processor,
 * because the stock {@code @PrimaryNodeOnly} annotation is {@code @Inherited} and could not be removed from a
 * subclass; a primary-node-only processor would starve trigger FlowFiles delivered to non-primary nodes.
 */
@TriggerSerially
@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({"azure", "microsoft", "cloud", "storage", "adlsgen2", "datalake"})
@SeeAlso({ListAzureDataLakeStorage.class, ADLSCredentialsControllerServiceExtended.class,
        FetchAzureDataLakeStorageExtended.class, DeleteAzureDataLakeStorageExtended.class})
@CapabilityDescription("Lists directory in an Azure Data Lake Storage Gen 2 filesystem. "
        + "This is a drop-in variant of ListAzureDataLakeStorage that additionally accepts incoming FlowFiles as trigger signals so the "
        + "listing can be wired downstream of another processor. When triggered by a FlowFile, that FlowFile's attributes are used to "
        + "evaluate Expression Language on the Filesystem Name, Directory Name, and ADLS Credentials (Storage Account Name) properties "
        + "and are copied onto every emitted listing FlowFile (listing attributes take precedence when names overlap). The trigger FlowFile "
        + "is removed so it is not emitted again with the listing results. Use ADLSCredentialsControllerServiceExtended when the storage "
        + "account name must be driven by trigger attributes (for example ${source_storage_account}). "
        + "File Filter and Path Filter support Expression Language against trigger FlowFile attributes; blank resolved "
        + "filter values are ignored. "
        + "When no incoming connection is present, the processor behaves like the standard "
        + "ListAzureDataLakeStorage and lists on its schedule. When the listing fails (e.g. the directory does not exist or the "
        + "credentials are invalid), an error FlowFile is routed to the 'Failure' relationship. When a Record Writer is configured, "
        + "a zero-record FlowFile is routed to 'No Files' only when the listing yields no matching files after filters are applied "
        + "(including an empty directory). "
        + "Note: unlike the standard processor, this variant is not annotated "
        + "@PrimaryNodeOnly so that it can be driven by an upstream connection; when running on a cluster without an incoming "
        + "connection, set the Execution Node to 'Primary Node' to avoid duplicate listings.")
@WritesAttributes({
        @WritesAttribute(attribute = ATTR_NAME_FILESYSTEM, description = ATTR_DESCRIPTION_FILESYSTEM),
        @WritesAttribute(attribute = ATTR_NAME_FILE_PATH, description = ATTR_DESCRIPTION_FILE_PATH),
        @WritesAttribute(attribute = ATTR_NAME_DIRECTORY, description = ATTR_DESCRIPTION_DIRECTORY),
        @WritesAttribute(attribute = ATTR_NAME_FILENAME, description = ATTR_DESCRIPTION_FILENAME),
        @WritesAttribute(attribute = ATTR_NAME_LENGTH, description = ATTR_DESCRIPTION_LENGTH),
        @WritesAttribute(attribute = ATTR_NAME_LAST_MODIFIED, description = ATTR_DESCRIPTION_LAST_MODIFIED),
        @WritesAttribute(attribute = ATTR_NAME_ETAG, description = ATTR_DESCRIPTION_ETAG)
})
@Stateful(scopes = {Scope.CLUSTER}, description = "After performing a listing of files, the timestamp of the newest file is stored. " +
        "This allows the Processor to list only files that have been added or modified after this date the next time that the Processor is run. State is " +
        "stored across the cluster so that this Processor can be run on Primary Node only and if a new Primary Node is selected, the new node can pick up " +
        "where the previous node left off, without duplicating the data.")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class ListAzureDataLakeStorageExtended extends AbstractListAzureProcessor<ADLSFileInfo> {

    /**
     * Regex validator that is aware of Expression Language: when the configured value contains EL (e.g.
     * {@code ${source_trigger_filename}}) the actual pattern is only known at runtime against FlowFile attributes, so the
     * literal is not compiled here. A blank value is treated as "no filter" since these properties are optional.
     */
    static final Validator EL_AWARE_REGULAR_EXPRESSION_VALIDATOR = (subject, input, context) -> {
        if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .valid(true)
                    .explanation("Expression Language present; pattern is evaluated at runtime.")
                    .build();
        }
        if (input == null || input.isBlank()) {
            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .valid(true)
                    .explanation("Empty value; no filter will be applied.")
                    .build();
        }
        try {
            Pattern.compile(input);
            return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
        } catch (final Exception e) {
            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .valid(false)
                    .explanation("Not a valid Java Regular Expression: " + e.getMessage())
                    .build();
        }
    };

    /**
     * Override the stock File Filter descriptor so the pattern can be driven by incoming trigger FlowFile attributes.
     */
    public static final PropertyDescriptor FILE_FILTER = new PropertyDescriptor.Builder()
            .name(ListAzureDataLakeStorage.FILE_FILTER.getName())
            .displayName(ListAzureDataLakeStorage.FILE_FILTER.getDisplayName())
            .description(ListAzureDataLakeStorage.FILE_FILTER.getDescription())
            .required(ListAzureDataLakeStorage.FILE_FILTER.isRequired())
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(EL_AWARE_REGULAR_EXPRESSION_VALIDATOR)
            .build();

    /**
     * Override the stock Path Filter descriptor so the pattern can be driven by incoming trigger FlowFile attributes.
     */
    public static final PropertyDescriptor PATH_FILTER = new PropertyDescriptor.Builder()
            .name(ListAzureDataLakeStorage.PATH_FILTER.getName())
            .displayName(ListAzureDataLakeStorage.PATH_FILTER.getDisplayName())
            .description(ListAzureDataLakeStorage.PATH_FILTER.getDescription())
            .required(ListAzureDataLakeStorage.PATH_FILTER.isRequired())
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(EL_AWARE_REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("An error FlowFile is routed here when the listing could not be performed "
                    + "(e.g. the directory does not exist or the credentials are invalid).")
            .autoTerminateDefault(true)
            .build();

    public static final Relationship REL_NO_FILES = new Relationship.Builder()
            .name("No Files")
            .description("A zero-record FlowFile when the listing yields no matching files after filters are applied "
                    + "(including an empty directory; requires Record Writer).")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            ADLS_CREDENTIALS_SERVICE,
            FILESYSTEM,
            DIRECTORY,
            RECURSE_SUBDIRECTORIES,
            FILE_FILTER,
            PATH_FILTER,
            INCLUDE_TEMPORARY_FILES,
            RECORD_WRITER,
            LISTING_STRATEGY,
            TRACKING_STATE_CACHE,
            TRACKING_TIME_WINDOW,
            INITIAL_LISTING_TARGET,
            MIN_AGE,
            MAX_AGE,
            MIN_SIZE,
            MAX_SIZE,
            AzureStorageUtils.PROXY_CONFIGURATION_SERVICE
    );

    private static final Set<PropertyDescriptor> LISTING_RESET_PROPERTIES = Set.of(
            ADLS_CREDENTIALS_SERVICE,
            FILESYSTEM,
            DIRECTORY,
            RECURSE_SUBDIRECTORIES,
            FILE_FILTER,
            PATH_FILTER,
            LISTING_STRATEGY
    );

    private volatile DataLakeServiceClientFactory clientFactory;

    /**
     * Snapshot of the last trigger FlowFile's attributes, used both for Expression Language during the listing and
     * for copying onto every emitted FlowFile. Always cleared in a {@code finally} block.
     */
    private final ThreadLocal<Map<String, String>> triggerFlowAttributes = new ThreadLocal<>();

    /** Set when {@link #performListing} fails during execution; the parent AbstractListProcessor swallows {@link IOException}. */
    private final ThreadLocal<Exception> listingFailure = new ThreadLocal<>();

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>(super.getRelationships());
        relationships.add(REL_FAILURE);
        relationships.add(REL_NO_FILES);
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        clientFactory = new DataLakeServiceClientFactory(getLogger(), AzureStorageUtils.getProxyOptions(context));
    }

    @OnStopped
    public void onStopped() {
        clientFactory = null;
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        super.migrateProperties(config);
        config.renameProperty(AzureStorageUtils.OLD_ADLS_CREDENTIALS_SERVICE_DESCRIPTOR_NAME, AzureStorageUtils.ADLS_CREDENTIALS_SERVICE.getName());
        config.renameProperty(AzureStorageUtils.OLD_FILESYSTEM_DESCRIPTOR_NAME, AzureStorageUtils.FILESYSTEM.getName());
        config.renameProperty(AzureStorageUtils.OLD_DIRECTORY_DESCRIPTOR_NAME, DIRECTORY.getName());
        config.renameProperty("recurse-subdirectories", RECURSE_SUBDIRECTORIES.getName());
        config.renameProperty("file-filter", FILE_FILTER.getName());
        config.renameProperty("path-filter", PATH_FILTER.getName());
        config.renameProperty("include-temporary-files", INCLUDE_TEMPORARY_FILES.getName());
        config.renameProperty(ListedEntityTracker.OLD_TRACKING_STATE_CACHE_PROPERTY_NAME, TRACKING_STATE_CACHE.getName());
        config.renameProperty(ListedEntityTracker.OLD_TRACKING_TIME_WINDOW_PROPERTY_NAME, TRACKING_TIME_WINDOW.getName());
        config.renameProperty(ListedEntityTracker.OLD_INITIAL_LISTING_TARGET_PROPERTY_NAME, INITIAL_LISTING_TARGET.getName());
    }

    @Override
    protected void customValidate(final ValidationContext context, final Collection<ValidationResult> results) {
        if (context.getProperty(PATH_FILTER).isSet() && !context.getProperty(RECURSE_SUBDIRECTORIES).asBoolean()) {
            results.add(new ValidationResult.Builder()
                    .subject(PATH_FILTER.getDisplayName())
                    .valid(false)
                    .explanation(String.format("'%s' cannot be set when '%s' is false", PATH_FILTER.getDisplayName(), RECURSE_SUBDIRECTORIES.getDisplayName()))
                    .build());
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        Map<String, String> triggerAttributes = null;

        if (context.hasIncomingConnection()) {
            final FlowFile incoming = session.get();

            // No work to do yet: an incoming connection is wired but empty. Yield rather than perform a listing,
            // so the processor stays strictly trigger-driven when wired downstream of another processor.
            if (incoming == null) {
                if (context.hasNonLoopConnection()) {
                    context.yield();
                    return;
                }
            } else {
                triggerAttributes = new HashMap<>(incoming.getAttributes());
                session.remove(incoming);
            }
        }

        triggerFlowAttributes.set(triggerAttributes);
        final ProcessSession listingSession = withTriggerAttributes(session, triggerAttributes);
        try {
            runListing(context, listingSession);

            // The parent AbstractListProcessor swallows listing IOExceptions (logs + yields), so a failed listing
            // is detected through the ThreadLocal set by performListing rather than a catch block.
            final Exception listingError = listingFailure.get();
            if (listingError != null) {
                listingFailure.remove();
                transferErrorFlowFile(listingSession, context, listingError);
                return;
            }

            emitEmptyFlowFileIfNeeded(context, listingSession);
        } catch (final Exception e) {
            getLogger().error("Failed to perform Azure Data Lake listing or handle FlowFiles due to: {}", e.getMessage(), e);
            transferErrorFlowFile(listingSession, context, e);
        } finally {
            listingFailure.remove();
            triggerFlowAttributes.remove();
        }
    }

    private void transferErrorFlowFile(final ProcessSession session, final ProcessContext context, final Exception error) {
        FlowFile errorFlowFile = session.create();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("error.message", error.getMessage());
        attributes.put("error.class", error.getClass().getName());
        putResolvedTargetAttributes(attributes, context);

        errorFlowFile = session.putAllAttributes(errorFlowFile, attributes);
        session.transfer(errorFlowFile, REL_FAILURE);

        getLogger().debug("Created error FlowFile with attributes: {}", attributes);
    }

    /**
     * Adds the EL-resolved filesystem/directory to the given attribute map. Resolution failures are ignored so this
     * never masks the original error being reported.
     */
    private void putResolvedTargetAttributes(final Map<String, String> attributes, final ProcessContext context) {
        final Map<String, String> elAttributes = triggerFlowAttributes.get() != null ? triggerFlowAttributes.get() : Map.of();
        try {
            attributes.put("azure.filesystem", context.getProperty(FILESYSTEM).evaluateAttributeExpressions(elAttributes).getValue());
            attributes.put("azure.directory", context.getProperty(DIRECTORY).evaluateAttributeExpressions(elAttributes).getValue());
        } catch (final Exception e) {
            getLogger().debug("Could not resolve filesystem/directory for FlowFile attributes: {}", e.getMessage());
        }
    }

    /**
     * Whether the current listing configuration would yield at least one matching file (after file/path/age/size
     * filters). Exposed as a protected method so tests can stub the remote check without contacting Azure.
     */
    protected boolean hasMatchingListingResults(final ProcessContext context) throws IOException {
        return !performListing(context, null, ListingMode.EXECUTION, true).isEmpty();
    }

    private void emitEmptyFlowFileIfNeeded(final ProcessContext context, final ProcessSession session) {
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER)
                .asControllerService(RecordSetWriterFactory.class);

        if (writerFactory == null) {
            getLogger().debug("No Record Writer Factory configured; skipping emission for empty directory.");
            return;
        }

        try {
            if (hasMatchingListingResults(context)) {
                return;
            }
        } catch (final IOException e) {
            getLogger().warn("Could not re-check listing for empty-state detection: {}", e.getMessage());
            return;
        }

        FlowFile emptyFlowFile = session.create();
        final Map<String, String> attributes = new HashMap<>();

        try {
            final RecordSchema schema = getRecordSchema();

            emptyFlowFile = session.write(emptyFlowFile, (final OutputStream out) -> {
                try (final RecordSetWriter writer =
                             writerFactory.createWriter(getLogger(), schema, out, Collections.emptyMap())) {
                    writer.beginRecordSet();
                    final WriteResult result = writer.finishRecordSet();
                    attributes.put("mime.type", writer.getMimeType());
                    attributes.put("record.count", String.valueOf(result.getRecordCount()));
                } catch (final SchemaNotFoundException snfe) {
                    throw new IOException("Schema not found for RecordSetWriter", snfe);
                }
            });

            putResolvedTargetAttributes(attributes, context);

            emptyFlowFile = session.putAllAttributes(emptyFlowFile, attributes);

            session.transfer(emptyFlowFile, REL_NO_FILES);
            getLogger().debug("Emitted zero-record FlowFile to No Files because no files matched the listing filters.");
        } catch (final Exception e) {
            getLogger().error("Failed to write empty listing FlowFile due to an error.", e);
            session.remove(emptyFlowFile);
        }
    }

    /**
     * Seam that performs the actual listing cycle. By default this delegates to the stock
     * {@code AbstractListProcessor#onTrigger} logic. Exposed as a protected method primarily so the trigger-handling
     * and attribute-merging behavior of this class can be unit tested without requiring a live Azure endpoint.
     */
    protected void runListing(final ProcessContext context, final ProcessSession session) {
        super.onTrigger(context, session);
    }

    /**
     * Attributes of the trigger FlowFile for the current onTrigger invocation, or {@code null} when the listing was
     * started by the schedule rather than an incoming FlowFile.
     */
    protected Map<String, String> getTriggerFlowAttributes() {
        return triggerFlowAttributes.get();
    }

    /**
     * Wrap the session so that every FlowFile created during the listing starts out with the trigger FlowFile's
     * attributes. Listing-specific attributes added afterwards via {@code putAllAttributes} take precedence when
     * names overlap, matching the behavior of the other extended List processors.
     */
    private static ProcessSession withTriggerAttributes(final ProcessSession session,
                                                        final Map<String, String> triggerAttributes) {
        if (triggerAttributes == null || triggerAttributes.isEmpty()) {
            return session;
        }

        final InvocationHandler handler = (proxy, method, args) -> {
            if ("putAllAttributes".equals(method.getName()) && args != null && args.length == 2 && args[1] instanceof Map) {
                @SuppressWarnings("unchecked")
                final Map<String, String> listingAttributes = (Map<String, String>) args[1];
                final Map<String, String> merged = new HashMap<>(triggerAttributes);
                merged.putAll(listingAttributes);
                args[1] = merged;
            }
            return method.invoke(session, args);
        };

        return (ProcessSession) Proxy.newProxyInstance(
                ProcessSession.class.getClassLoader(),
                new Class<?>[] {ProcessSession.class},
                handler);
    }

    @Override
    protected RecordSchema getRecordSchema() {
        return ADLSFileInfo.getRecordSchema();
    }

    @Override
    protected Scope getStateScope(final PropertyContext context) {
        return Scope.CLUSTER;
    }

    @Override
    protected String getDefaultTimePrecision() {
        return PRECISION_MILLIS.getValue();
    }

    @Override
    protected boolean isListingResetNecessary(final PropertyDescriptor property) {
        return LISTING_RESET_PROPERTIES.contains(property);
    }

    @Override
    protected String getPath(final ProcessContext context) {
        // Unlike the stock processor, evaluate against the trigger FlowFile attributes (null map falls back to
        // environment-only evaluation, matching the stock behavior when there is no trigger FlowFile).
        final String directory = context.getProperty(DIRECTORY).evaluateAttributeExpressions(triggerFlowAttributes.get()).getValue();
        return directory != null ? directory : ".";
    }

    @Override
    protected List<ADLSFileInfo> performListing(final ProcessContext context, final Long minTimestamp, final ListingMode listingMode) throws IOException {
        return performListing(context, minTimestamp, listingMode, true);
    }

    @Override
    protected Integer countUnfilteredListing(final ProcessContext context) throws IOException {
        return performListing(context, null, ListingMode.CONFIGURATION_VERIFICATION, false).size();
    }

    @Override
    protected String getListingContainerName(final ProcessContext context) {
        return String.format("Azure Data Lake Directory [%s]", getPath(context));
    }

    @Override
    protected Map<String, String> createAttributes(final ADLSFileInfo fileInfo, final ProcessContext context) {
        final Map<String, String> attributes = new HashMap<>();

        attributes.put(ATTR_NAME_FILESYSTEM, fileInfo.getFileSystem());
        attributes.put(ATTR_NAME_FILE_PATH, fileInfo.getFilePath());
        attributes.put(ATTR_NAME_DIRECTORY, fileInfo.getDirectory());
        attributes.put(ATTR_NAME_FILENAME, fileInfo.getFilename());
        attributes.put(ATTR_NAME_LENGTH, String.valueOf(fileInfo.getLength()));
        attributes.put(ATTR_NAME_LAST_MODIFIED, String.valueOf(fileInfo.getLastModified()));
        attributes.put(ATTR_NAME_ETAG, fileInfo.getEtag());

        return attributes;
    }

    private List<ADLSFileInfo> performListing(final ProcessContext context, final Long minTimestamp, final ListingMode listingMode,
                                              final boolean applyFilters) throws IOException {
        final DataLakeServiceClientFactory currentClientFactory;
        if (ListingMode.CONFIGURATION_VERIFICATION == listingMode) {
            currentClientFactory = new DataLakeServiceClientFactory(getLogger(), getProxyOptions(context));
        } else {
            currentClientFactory = clientFactory;
        }

        try {
            // Unlike the stock processor, Filesystem Name and Directory Name Expression Language is resolved against
            // the trigger FlowFile attributes when the listing was started by an incoming FlowFile.
            final Map<String, String> elAttributes = triggerFlowAttributes.get();
            final String fileSystem = evaluateFileSystemProperty(FILESYSTEM, context, elAttributes);
            final String baseDirectory = evaluateDirectoryProperty(DIRECTORY, context, elAttributes);
            final boolean recurseSubdirectories = context.getProperty(RECURSE_SUBDIRECTORIES).asBoolean();

            final Pattern filePattern = compileFilterOrNull(context.getProperty(FILE_FILTER), elAttributes);
            final Pattern pathPattern = compileFilterOrNull(context.getProperty(PATH_FILTER), elAttributes);

            final ADLSCredentialsService credentialsService = context.getProperty(ADLS_CREDENTIALS_SERVICE).asControllerService(ADLSCredentialsService.class);

            final Map<String, String> credentialAttributes = elAttributes != null ? elAttributes : Map.of();
            final ADLSCredentialsDetails credentialsDetails = credentialsService.getCredentialsDetails(credentialAttributes);

            final DataLakeServiceClient storageClient = currentClientFactory.getStorageClient(credentialsDetails);
            final DataLakeFileSystemClient fileSystemClient = storageClient.getFileSystemClient(fileSystem);

            final ListPathsOptions options = new ListPathsOptions();
            options.setPath(baseDirectory);
            options.setRecursive(recurseSubdirectories);

            final Pattern baseDirectoryPattern = Pattern.compile("^" + baseDirectory + "/?");
            final boolean includeTempFiles = context.getProperty(INCLUDE_TEMPORARY_FILES).asBoolean();
            final long minimumTimestamp = minTimestamp == null ? 0 : minTimestamp;

            final List<ADLSFileInfo> listing = fileSystemClient.listPaths(options, null).stream()
                    .filter(pathItem -> !pathItem.isDirectory())
                    .filter(pathItem -> includeTempFiles || !pathItem.getName().contains(TEMP_FILE_DIRECTORY))
                    .filter(pathItem -> isFileInfoMatchesWithAgeAndSize(context, minimumTimestamp, pathItem.getLastModified().toInstant().toEpochMilli(), pathItem.getContentLength()))
                    .map(pathItem -> new ADLSFileInfo.Builder()
                            .fileSystem(fileSystem)
                            .filePath(pathItem.getName())
                            .length(pathItem.getContentLength())
                            .lastModified(pathItem.getLastModified().toInstant().toEpochMilli())
                            .etag(pathItem.getETag())
                            .build())
                    .toList();

            if (!applyFilters) {
                return listing;
            }

            return listing.stream()
                    .filter(fileInfo -> filePattern == null || filePattern.matcher(fileInfo.getFilename()).matches())
                    .filter(fileInfo -> pathPattern == null || pathPattern.matcher(RegExUtils.removeFirst((CharSequence) fileInfo.getDirectory(), baseDirectoryPattern)).matches())
                    .toList();
        } catch (final Exception e) {
            getLogger().error("Failed to list directory on Azure Data Lake Storage", e);
            if (listingMode == ListingMode.EXECUTION) {
                listingFailure.set(e);
            }
            throw new IOException(ExceptionUtils.getRootCause(e));
        }
    }

    /**
     * Resolves a regex filter property against the supplied FlowFile attributes and compiles it. Returns {@code null}
     * (meaning "no filter") when the property is unset or its resolved value is empty/blank.
     */
    static Pattern compileFilterOrNull(final PropertyValue property, final Map<String, String> elAttributes) {
        if (!property.isSet()) {
            return null;
        }
        final PropertyValue resolved;
        if (elAttributes != null && !elAttributes.isEmpty()) {
            resolved = property.evaluateAttributeExpressions(elAttributes);
        } else if (property.isExpressionLanguagePresent()) {
            // FLOWFILE_ATTRIBUTES-scoped filters cannot be resolved until a trigger FlowFile is available.
            return null;
        } else {
            resolved = property.evaluateAttributeExpressions();
        }
        final String value = resolved.getValue();
        if (value == null || value.isBlank()) {
            return null;
        }
        return Pattern.compile(value);
    }
}
