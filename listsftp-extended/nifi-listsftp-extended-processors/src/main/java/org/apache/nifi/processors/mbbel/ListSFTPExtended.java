// java
package org.apache.nifi.processors.mbbel;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.processor.util.file.transfer.FileInfo;
import org.apache.nifi.processor.util.file.transfer.FileTransfer;
import org.apache.nifi.processor.util.file.transfer.ListFileTransfer;
import org.apache.nifi.processor.util.list.ListedEntityTracker;
import org.apache.nifi.processors.standard.FetchSFTP;
import org.apache.nifi.processors.standard.GetSFTP;
import org.apache.nifi.processors.standard.ListFile;
import org.apache.nifi.processors.standard.PutSFTP;
import org.apache.nifi.processors.standard.util.FTPTransfer;
import org.apache.nifi.processors.standard.util.SFTPTransfer;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.schema.access.SchemaNotFoundException;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@TriggerSerially
@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({"list", "sftp", "remote", "ingest", "source", "input", "files"})
@CapabilityDescription("Performs a listing of the files residing on an SFTP server. For each file that is found on the remote server, a new FlowFile will be created with the filename attribute "
        + "set to the name of the file on the remote server. This can then be used in conjunction with FetchSFTP in order to fetch those files. "
        + "This processor can optionally accept incoming FlowFiles as trigger signals. Trigger attributes are used for "
        + "Expression Language and are copied onto each emitted listing FlowFile; listing attributes take precedence when names overlap. "
        + "The trigger FlowFile is removed so it is not emitted again with listing results. "
        + "When a Record Writer is configured, a zero-record FlowFile is routed to No Files only when the remote directory has no file entries. "
        + "Password is non-sensitive and supports dynamic authentication: when the resolved Password, Private Key Path, or Private Key Passphrase "
        + "is blank it is treated as unset so the other credential can be used. File Filter Regex and Path Filter Regex support Expression Language "
        + "against trigger FlowFile attributes; blank resolved filter values are ignored.")
@SeeAlso({FetchSFTP.class, GetSFTP.class, PutSFTP.class})
@WritesAttributes({
        @WritesAttribute(attribute = "sftp.remote.host", description = "The hostname of the SFTP Server"),
        @WritesAttribute(attribute = "sftp.remote.port", description = "The port that was connected to on the SFTP Server"),
        @WritesAttribute(attribute = "sftp.listing.user", description = "The username of the user that performed the SFTP Listing"),
        @WritesAttribute(attribute = ListFile.FILE_OWNER_ATTRIBUTE, description = "The numeric owner id of the source file"),
        @WritesAttribute(attribute = ListFile.FILE_GROUP_ATTRIBUTE, description = "The numeric group id of the source file"),
        @WritesAttribute(attribute = ListFile.FILE_PERMISSIONS_ATTRIBUTE, description = "The read/write/execute permissions of the source file"),
        @WritesAttribute(attribute = ListFile.FILE_SIZE_ATTRIBUTE, description = "The number of bytes in the source file"),
        @WritesAttribute(attribute = ListFile.FILE_LAST_MODIFY_TIME_ATTRIBUTE, description = "The timestamp of when the file in the filesystem was" +
                "last modified as 'yyyy-MM-dd'T'HH:mm:ssZ'"),
        @WritesAttribute(attribute = "filename", description = "The name of the file on the SFTP Server"),
        @WritesAttribute(attribute = "path", description = "The fully qualified name of the directory on the SFTP Server from which the file was pulled"),
        @WritesAttribute(attribute = "mime.type", description = "The MIME Type that is provided by the configured Record Writer"),
})
@Stateful(scopes = {Scope.CLUSTER}, description = "After performing a listing of files, the timestamp of the newest file is stored. "
        + "This allows the Processor to list only files that have been added or modified after "
        + "this date the next time that the Processor is run. State is stored across the cluster to prevent duplicate listings even when the processor runs on multiple nodes.")
public class ListSFTPExtended extends ListFileTransfer {

    /**
     * Snapshot of the last trigger FlowFile's attributes for EL during listing and for copying onto emitted FlowFiles. Cleared in {@code finally}.
     */
    private final ThreadLocal<Map<String, String>> triggerFlowAttributes = new ThreadLocal<>();
    private volatile Predicate<FileInfo> fileFilter;

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(SFTPTransfer.PASSWORD)
            .sensitive(false)
            .build();

    /**
     * Regex validator that is aware of Expression Language: when the configured value contains EL (e.g.
     * {@code ${file_filter}}) the actual pattern is only known at runtime against FlowFile attributes, so the literal
     * is not compiled here. A blank value is treated as "no filter" since these properties are optional.
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
     * Override the stock REMOTE_PATH descriptor so the directory path can be driven by incoming trigger FlowFile
     * attributes (e.g. {@code ${input.path}}). The stock descriptor only supports ENVIRONMENT-scope EL.
     */
    public static final PropertyDescriptor REMOTE_PATH = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(ListFileTransfer.REMOTE_PATH)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor FILE_FILTER_REGEX = new PropertyDescriptor.Builder()
            .name(FileTransfer.FILE_FILTER_REGEX.getName())
            .displayName(FileTransfer.FILE_FILTER_REGEX.getDisplayName())
            .description(FileTransfer.FILE_FILTER_REGEX.getDescription())
            .required(FileTransfer.FILE_FILTER_REGEX.isRequired())
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(EL_AWARE_REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final PropertyDescriptor PATH_FILTER_REGEX = new PropertyDescriptor.Builder()
            .name(FileTransfer.PATH_FILTER_REGEX.getName())
            .displayName(FileTransfer.PATH_FILTER_REGEX.getDisplayName())
            .description(FileTransfer.PATH_FILTER_REGEX.getDescription())
            .required(FileTransfer.PATH_FILTER_REGEX.isRequired())
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(EL_AWARE_REGULAR_EXPRESSION_VALIDATOR)
            .build();

    private static final Set<String> BLANK_AS_UNSET_PROPERTY_NAMES = Set.of(
            PASSWORD.getName(),
            SFTPTransfer.PRIVATE_KEY_PATH.getName(),
            SFTPTransfer.PRIVATE_KEY_PASSPHRASE.getName(),
            FILE_FILTER_REGEX.getName(),
            PATH_FILTER_REGEX.getName()
    );

    private static final Map<String, PropertyDescriptor> OVERRIDDEN_DESCRIPTORS_BY_NAME = Map.of(
            PASSWORD.getName(), PASSWORD,
            REMOTE_PATH.getName(), REMOTE_PATH,
            FILE_FILTER_REGEX.getName(), FILE_FILTER_REGEX,
            PATH_FILTER_REGEX.getName(), PATH_FILTER_REGEX
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            FILE_TRANSFER_LISTING_STRATEGY,
            SFTPTransfer.HOSTNAME,
            SFTPTransfer.PORT,
            SFTPTransfer.USERNAME,
            PASSWORD,
            SFTPTransfer.PRIVATE_KEY_PATH,
            SFTPTransfer.PRIVATE_KEY_PASSPHRASE,
            REMOTE_PATH,
            RECORD_WRITER,
            SFTPTransfer.RECURSIVE_SEARCH,
            SFTPTransfer.FOLLOW_SYMLINK,
            FILE_FILTER_REGEX,
            PATH_FILTER_REGEX,
            SFTPTransfer.IGNORE_DOTTED_FILES,
            SFTPTransfer.STRICT_HOST_KEY_CHECKING,
            SFTPTransfer.HOST_KEY_FILE,
            SFTPTransfer.CONNECTION_TIMEOUT,
            SFTPTransfer.DATA_TIMEOUT,
            SFTPTransfer.USE_KEEPALIVE_ON_TIMEOUT,
            TARGET_SYSTEM_TIMESTAMP_PRECISION,
            SFTPTransfer.USE_COMPRESSION,
            SFTPTransfer.PROXY_CONFIGURATION_SERVICE,
            ListedEntityTracker.TRACKING_STATE_CACHE,
            ListedEntityTracker.TRACKING_TIME_WINDOW,
            ListedEntityTracker.INITIAL_LISTING_TARGET,
            ListFile.MIN_AGE,
            ListFile.MAX_AGE,
            ListFile.MIN_SIZE,
            ListFile.MAX_SIZE,
            SFTPTransfer.ALGORITHM_CONFIGURATION,
            SFTPTransfer.CIPHERS_ALLOWED,
            SFTPTransfer.KEY_ALGORITHMS_ALLOWED,
            SFTPTransfer.KEY_EXCHANGE_ALGORITHMS_ALLOWED,
            SFTPTransfer.MESSAGE_AUTHENTICATION_CODES_ALLOWED
    );


    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("Failed processing")
            .autoTerminateDefault(true)
            .build();

    public static final Relationship REL_NO_FILES = new Relationship.Builder()
            .name("No Files")
            .description("A zero-record FlowFile when the listing yields no matching files after filters are applied "
                    + "(including an empty remote directory; requires Record Writer).")
            .build();

    @Override
    public Set<Relationship> getRelationships() {
        return new HashSet<>() {{
            add(REL_FAILURE);
            add(REL_NO_FILES);
            add(REL_SUCCESS);
        }};
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        super.migrateProperties(config);
        FTPTransfer.migrateProxyProperties(config);
        config.removeProperty(FileTransfer.REMOTE_POLL_BATCH_SIZE.getName());
        SFTPTransfer.migrateAlgorithmProperties(config);
        config.renameProperty(SFTPTransfer.OLD_FOLLOW_SYMLINK_PROPERTY_NAME, SFTPTransfer.FOLLOW_SYMLINK.getName());
        config.renameProperty(ListedEntityTracker.OLD_TRACKING_STATE_CACHE_PROPERTY_NAME, ListedEntityTracker.TRACKING_STATE_CACHE.getName());
        config.renameProperty(ListedEntityTracker.OLD_TRACKING_TIME_WINDOW_PROPERTY_NAME, ListedEntityTracker.TRACKING_TIME_WINDOW.getName());
        config.renameProperty(ListedEntityTracker.OLD_INITIAL_LISTING_TARGET_PROPERTY_NAME, ListedEntityTracker.INITIAL_LISTING_TARGET.getName());
    }

    @Override
    protected FileTransfer getFileTransfer(final ProcessContext context) {
        final ProcessContext wrappedContext = new FlowFileAwareProcessContext(context, triggerFlowAttributes);
        return new SFTPTransfer(wrappedContext, getLogger());
    }

    @Override
    protected String getProtocolName() {
        return "sftp";
    }

    @Override
    protected Map<String, String> createAttributes(final FileInfo fileInfo, final ProcessContext context) {
        final Map<String, String> attributes = new HashMap<>();
        copyTriggerAttributes(attributes);
        attributes.putAll(super.createAttributes(fileInfo, context));
        return attributes;
    }

    @Override
    protected Scope getStateScope(final PropertyContext context) {
        // Use cluster scope so that component can be run on Primary Node Only and can still
        // pick up where it left off, even if the Primary Node changes.
        return Scope.CLUSTER;
    }

    @Override
    protected void customValidate(ValidationContext validationContext, Collection<ValidationResult> results) {
        SFTPTransfer.validateProxySpec(validationContext, results);
        // Note: Expression language properties are evaluated at runtime via FlowFileAwareProcessContext
    }

    @Override
    protected List<FileInfo> performListing(final ProcessContext context, final Long minTimestamp, final ListingMode listingMode,
                                            final boolean applyFilters) throws IOException {
        final List<FileInfo> listing = super.performListing(context, minTimestamp, listingMode, applyFilters);

        if (!applyFilters) {
            return listing;
        }

        final Predicate<FileInfo> filePredicate = listingMode == ListingMode.EXECUTION ? this.fileFilter : createFileFilter(context);
        return listing.stream()
                .filter(filePredicate)
                .collect(Collectors.toList());
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        fileFilter = createFileFilter(context);
    }

    private Predicate<FileInfo> createFileFilter(final ProcessContext context) {
        final long minSize = context.getProperty(ListFile.MIN_SIZE).asDataSize(DataUnit.B).longValue();
        final Double maxSize = context.getProperty(ListFile.MAX_SIZE).asDataSize(DataUnit.B);
        final long minAge = context.getProperty(ListFile.MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final Long maxAge = context.getProperty(ListFile.MAX_AGE).asTimePeriod(TimeUnit.MILLISECONDS);

        return (attributes) -> {
            if (attributes.isDirectory()) {
                return true;
            }

            if (minSize > attributes.getSize()) {
                return false;
            }
            if (maxSize != null && maxSize < attributes.getSize()) {
                return false;
            }
            final long fileAge = System.currentTimeMillis() - attributes.getLastModifiedTime();
            if (minAge > fileAge) {
                return false;
            }
            if (maxAge != null && maxAge < fileAge) {
                return false;
            }

            return true;
        };
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        FlowFile incoming = null;

        // Handle incoming FlowFiles if there are incoming connections
        if (context.hasIncomingConnection()) {
            incoming = session.get();

            if (incoming == null && context.hasNonLoopConnection()) {
                context.yield();
                return;
            }

            if (incoming != null) {
                triggerFlowAttributes.set(new HashMap<>(incoming.getAttributes()));
                session.remove(incoming);
                incoming = null;
            }
        }

        try {
            final ProcessSession listingSession = withTriggerAttributes(session, triggerFlowAttributes.get());
            super.onTrigger(context, listingSession);

            emitEmptyFlowFileIfNeeded(context, listingSession);
        } catch (final Exception e) {
            getLogger().error("Failed to perform SFTP listing or handle FlowFiles due to: {}", e.getMessage(), e);

            final ProcessSession listingSession = withTriggerAttributes(session, triggerFlowAttributes.get());
            final FlowFile errorFlowFile = createErrorFlowFile(listingSession, context, e);
            if (errorFlowFile != null) {
                listingSession.transfer(errorFlowFile, REL_FAILURE);
            }
        } finally {
            triggerFlowAttributes.remove();
        }
    }

    private FlowFile createErrorFlowFile(final ProcessSession session,
                                         final ProcessContext context,
                                         final Exception error) {
        FlowFile errorFlowFile = session.create();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("error.message", error.getMessage());
        attributes.put("error.class", error.getClass().getName());
        final Map<String, String> elAttributes = triggerFlowAttributes.get() != null ? triggerFlowAttributes.get() : Map.of();
        attributes.put("sftp.remote.host", context.getProperty(SFTPTransfer.HOSTNAME)
                .evaluateAttributeExpressions(elAttributes).getValue());
        attributes.put("sftp.remote.port", context.getProperty(SFTPTransfer.PORT)
                .evaluateAttributeExpressions(elAttributes).getValue());
        attributes.put("sftp.listing.user", context.getProperty(SFTPTransfer.USERNAME)
                .evaluateAttributeExpressions(elAttributes).getValue());

        errorFlowFile = session.putAllAttributes(errorFlowFile, attributes);

        getLogger().debug("Created error FlowFile with attributes: {}", attributes);

        return errorFlowFile;
    }

    private void copyTriggerAttributes(final Map<String, String> attributes) {
        final Map<String, String> triggerAttrs = triggerFlowAttributes.get();
        if (triggerAttrs != null && !triggerAttrs.isEmpty()) {
            attributes.putAll(triggerAttrs);
        }
    }

    private static ProcessSession withTriggerAttributes(final ProcessSession session,
                                                        final Map<String, String> triggerAttributes) {
        if (triggerAttributes == null || triggerAttributes.isEmpty()) {
            return session;
        }

        final InvocationHandler handler = new InvocationHandler() {
            @Override
            public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
                if ("putAllAttributes".equals(method.getName()) && args != null && args.length == 2 && args[1] instanceof Map) {
                    @SuppressWarnings("unchecked")
                    final Map<String, String> merged = new HashMap<>(triggerAttributes);
                    merged.putAll((Map<String, String>) args[1]);
                    args[1] = merged;
                }
                return method.invoke(session, args);
            }
        };

        return (ProcessSession) Proxy.newProxyInstance(
                ProcessSession.class.getClassLoader(),
                new Class<?>[] {ProcessSession.class},
                handler);
    }

    private void emitEmptyFlowFileIfNeeded(final ProcessContext context,
                                           final ProcessSession session) {

        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER)
                .asControllerService(RecordSetWriterFactory.class);

        if (writerFactory == null) {
            getLogger().debug("No Record Writer Factory configured; skipping emission when no files match.");
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
        final Map<String, String> elAttributes = triggerFlowAttributes.get() != null
                ? triggerFlowAttributes.get()
                : Map.of();

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

            attributes.put("sftp.remote.host",
                    context.getProperty(SFTPTransfer.HOSTNAME)
                            .evaluateAttributeExpressions(elAttributes).getValue());
            attributes.put("sftp.remote.port",
                    context.getProperty(SFTPTransfer.PORT)
                            .evaluateAttributeExpressions(elAttributes).getValue());
            attributes.put("sftp.listing.user",
                    context.getProperty(SFTPTransfer.USERNAME)
                            .evaluateAttributeExpressions(elAttributes).getValue());

            emptyFlowFile = session.putAllAttributes(emptyFlowFile, attributes);

            session.transfer(emptyFlowFile, REL_NO_FILES);
            getLogger().debug("Emitted zero-record FlowFile to No Files because no files matched the listing filters.");
        } catch (final Exception e) {
            getLogger().error("Failed to write empty listing FlowFile due to an error.", e);
            session.remove(emptyFlowFile);
        }
    }

    /**
     * Whether the current listing configuration would yield at least one matching file entry (after file/age/size
     * filters). Exposed as a protected method so tests can stub the remote check without contacting SFTP.
     */
    protected boolean hasMatchingListingResults(final ProcessContext context) throws IOException {
        final List<FileInfo> listing = performListing(context, null, ListingMode.EXECUTION, true);
        return listing.stream().anyMatch(info -> !info.isDirectory());
    }

    /**
     * Resolves a regex filter property against the supplied FlowFile attributes and compiles it. Returns {@code null}
     * (meaning "no filter") when the property is unset or its resolved value is empty/blank.
     */
    static Pattern compileFilterOrNull(final PropertyValue property, final Map<String, String> elAttributes) {
        if (!property.isSet()) {
            return null;
        }
        final String value = property.evaluateAttributeExpressions(elAttributes).getValue();
        if (value == null || value.isBlank()) {
            return null;
        }
        return Pattern.compile(value);
    }

    private static class FlowFileAwareProcessContext implements ProcessContext {
        private final ProcessContext delegate;
        private final ThreadLocal<Map<String, String>> attributeSnapshot;

        FlowFileAwareProcessContext(final ProcessContext delegate, final ThreadLocal<Map<String, String>> attributeSnapshot) {
            this.delegate = delegate;
            this.attributeSnapshot = attributeSnapshot;
        }

        @Override
        public PropertyValue getProperty(final PropertyDescriptor descriptor) {
            final PropertyDescriptor resolvedDescriptor = descriptor == null
                    ? null
                    : OVERRIDDEN_DESCRIPTORS_BY_NAME.getOrDefault(descriptor.getName(), descriptor);
            final PropertyValue delegateValue = delegate.getProperty(resolvedDescriptor);
            final Map<String, String> attrs = attributeSnapshot.get();
            PropertyValue value = delegateValue;
            if (attrs != null && !attrs.isEmpty()
                    && resolvedDescriptor != null
                    && resolvedDescriptor.getExpressionLanguageScope() == ExpressionLanguageScope.FLOWFILE_ATTRIBUTES) {
                value = delegateValue.evaluateAttributeExpressions(attrs);
            }
            if (resolvedDescriptor != null && BLANK_AS_UNSET_PROPERTY_NAMES.contains(resolvedDescriptor.getName())) {
                value = SftpBlankAsUnsetSupport.blankAsUnset(value);
            }
            return value;
        }

        @Override
        public Map<String, String> getAllProperties() {
            return delegate.getAllProperties();
        }

        @Override
        public PropertyValue getProperty(final String propertyName) {
            return delegate.getProperty(propertyName);
        }

        @Override
        public PropertyValue newPropertyValue(final String rawValue) {
            return delegate.newPropertyValue(rawValue);
        }

        @Override
        public java.util.Map<PropertyDescriptor, String> getProperties() {
            return delegate.getProperties();
        }

        @Override
        public void yield() {
            delegate.yield();
        }

        @Override
        public boolean hasIncomingConnection() {
            return delegate.hasIncomingConnection();
        }

        @Override
        public boolean hasNonLoopConnection() {
            return delegate.hasNonLoopConnection();
        }

        @Override
        public boolean hasConnection(Relationship relationship) {
            return delegate.hasConnection(relationship);
        }

        @Override
        public boolean isExpressionLanguagePresent(PropertyDescriptor propertyDescriptor) {
            return delegate.isExpressionLanguagePresent(propertyDescriptor);
        }

        @Override
        public StateManager getStateManager() {
            return delegate.getStateManager();
        }

        @Override
        public String getName() {
            return delegate.getName();
        }

        @Override
        public boolean isRelationshipRetried(Relationship relationship) {
            return delegate.isRelationshipRetried(relationship);
        }

        @Override
        public int getRetryCount() {
            return delegate.getRetryCount();
        }

        @Override
        public int getMaxConcurrentTasks() {
            return delegate.getMaxConcurrentTasks();
        }

        @Override
        public ExecutionNode getExecutionNode() {
            return delegate.getExecutionNode();
        }

        @Override
        public String getAnnotationData() {
            return delegate.getAnnotationData();
        }

        @Override
        public ControllerServiceLookup getControllerServiceLookup() {
            return delegate.getControllerServiceLookup();
        }

        @Override
        public Set<Relationship> getAvailableRelationships() {
            return delegate.getAvailableRelationships();
        }

        @Override
        public boolean isAutoTerminated(Relationship relationship) {
            return delegate.isAutoTerminated(relationship);
        }

        @Override
        public boolean isConnectedToCluster() {
            return delegate.isConnectedToCluster();
        }
    }
}
