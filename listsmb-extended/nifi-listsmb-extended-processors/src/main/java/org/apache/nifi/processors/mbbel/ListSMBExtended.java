package org.apache.nifi.processors.mbbel;

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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processor.util.list.ListedEntityTracker;
import org.apache.nifi.processors.smb.FetchSmb;
import org.apache.nifi.processors.smb.GetSmbFile;
import org.apache.nifi.processors.smb.PutSmbFile;
import org.apache.nifi.processors.smb.util.InitialListingStrategy;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.services.smb.SmbjClientProviderServiceExtended;
import org.apache.nifi.services.smb.SmbClientProviderService;
import org.apache.nifi.services.smb.SmbClientService;
import org.apache.nifi.services.smb.SmbListableEntity;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.nifi.components.state.Scope.CLUSTER;
import static org.apache.nifi.processor.util.StandardValidators.DATA_SIZE_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.NON_BLANK_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.NON_EMPTY_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.TIME_PERIOD_VALIDATOR;
import static org.apache.nifi.services.smb.SmbListableEntity.ALLOCATION_SIZE;
import static org.apache.nifi.services.smb.SmbListableEntity.CHANGE_TIME;
import static org.apache.nifi.services.smb.SmbListableEntity.CREATION_TIME;
import static org.apache.nifi.services.smb.SmbListableEntity.FILENAME;
import static org.apache.nifi.services.smb.SmbListableEntity.LAST_ACCESS_TIME;
import static org.apache.nifi.services.smb.SmbListableEntity.LAST_MODIFIED_TIME;
import static org.apache.nifi.services.smb.SmbListableEntity.PATH;
import static org.apache.nifi.services.smb.SmbListableEntity.SERVICE_LOCATION;
import static org.apache.nifi.services.smb.SmbListableEntity.SHORT_NAME;
import static org.apache.nifi.services.smb.SmbListableEntity.SIZE;

@TriggerSerially
@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({"samba, smb, cifs, files", "list"})
@SeeAlso({PutSmbFile.class, GetSmbFile.class, FetchSmb.class, SmbjClientProviderServiceExtended.class})
@CapabilityDescription("Lists concrete files shared via SMB protocol. " +
        "Each listed file may result in one FlowFile, the metadata being written as FlowFile attributes. " +
        "Or - in case the 'Record Writer' property is set - the entire result is written as records to a single FlowFile. " +
        "Optional incoming FlowFiles can drive scheduling and Expression Language on properties. Trigger attributes are " +
        "copied onto each emitted listing FlowFile; listing attributes take precedence when names overlap. " +
        "The trigger FlowFile is removed so it is not emitted again with listing results. " +
        "When a Record Writer is configured, a zero-record FlowFile is routed to No Files only when the remote directory has no entries.")
@WritesAttributes({
        @WritesAttribute(attribute = FILENAME, description = "The name of the file that was read from filesystem."),
        @WritesAttribute(attribute = SHORT_NAME, description = "The short name of the file that was read from filesystem."),
        @WritesAttribute(attribute = PATH, description =
                "The path is set to the relative path of the file's directory on the remote filesystem compared to the "
                        + "Share root directory. For example, for a given remote location"
                        + "smb://HOSTNAME:PORT/SHARE/DIRECTORY, and a file is being listed from "
                        + "smb://HOSTNAME:PORT/SHARE/DIRECTORY/sub/folder/file then the path attribute will be set to "
                        + "\"DIRECTORY/sub/folder\"."),
        @WritesAttribute(attribute = SERVICE_LOCATION, description =
                "The SMB URL of the share."),
        @WritesAttribute(attribute = LAST_MODIFIED_TIME, description =
                "The timestamp of when the file's content changed in the filesystem as 'yyyy-MM-dd'T'HH:mm:ss'."),
        @WritesAttribute(attribute = CREATION_TIME, description =
                "The timestamp of when the file was created in the filesystem as 'yyyy-MM-dd'T'HH:mm:ss'."),
        @WritesAttribute(attribute = LAST_ACCESS_TIME, description =
                "The timestamp of when the file was accessed in the filesystem as 'yyyy-MM-dd'T'HH:mm:ss'."),
        @WritesAttribute(attribute = CHANGE_TIME, description =
                "The timestamp of when the file's attributes was changed in the filesystem as 'yyyy-MM-dd'T'HH:mm:ss'."),
        @WritesAttribute(attribute = SIZE, description = "The size of the file in bytes."),
        @WritesAttribute(attribute = ALLOCATION_SIZE, description = "The number of bytes allocated for the file on the server."),
        @WritesAttribute(attribute = "mime.type", description = "The MIME Type that is provided by the configured Record Writer"),
})
@Stateful(scopes = {Scope.CLUSTER}, description =
        "After performing a listing of files, the state of the previous listing can be stored in order to list files "
                + "continuously without duplication."
)
public class ListSMBExtended extends AbstractListProcessor<SmbListableEntity> {

    /**
     * Snapshot of the last trigger FlowFile's attributes for EL during listing and for copying onto emitted FlowFiles. Cleared in {@code finally}.
     * The trigger FlowFile itself is removed before listing so {@link AbstractListProcessor} internal {@code commitAsync} does not fail validation.
     */
    private final ThreadLocal<Map<String, String>> triggerFlowAttributes = new ThreadLocal<>();
    /** Set when {@link #performListing} fails during execution; parent swallows {@link IOException}. */
    private final ThreadLocal<Exception> listingFailure = new ThreadLocal<>();

    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("Input Directory")
            .description("The network folder from which to list files. This is the remaining relative path " +
                    "after the share: smb://HOSTNAME:PORT/SHARE/[DIRECTORY]/sub/directories. It is also possible "
                    + "to add subdirectories. The given path on the remote file share must exist. "
                    + "This can be checked using verification. You may mix Windows and Linux-style "
                    + "directory separators.")
            .required(false)
            .addValidator(NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor MINIMUM_AGE = new PropertyDescriptor.Builder()
            .name("Minimum File Age")
            .description("The minimum age that a file must be in order to be listed; any file younger than this "
                    + "amount of time will be ignored.")
            .required(true)
            .addValidator(TIME_PERIOD_VALIDATOR)
            .defaultValue("5 secs")
            .build();

    public static final PropertyDescriptor MAXIMUM_AGE = new PropertyDescriptor.Builder()
            .name("Maximum File Age")
            .description("Any file older than the given value will be omitted.")
            .required(false)
            .addValidator(TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor MINIMUM_SIZE = new PropertyDescriptor.Builder()
            .name("Minimum File Size")
            .description("Any file smaller than the given value will be omitted.")
            .required(false)
            .addValidator(DATA_SIZE_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAXIMUM_SIZE = new PropertyDescriptor.Builder()
            .name("Maximum File Size")
            .description("Any file larger than the given value will be omitted.")
            .required(false)
            .addValidator(DATA_SIZE_VALIDATOR)
            .build();

    public static final PropertyDescriptor SMB_LISTING_STRATEGY = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(LISTING_STRATEGY)
            .allowableValues(BY_ENTITIES, NO_TRACKING, BY_TIMESTAMPS)
            .build();

    public static final PropertyDescriptor INITIAL_LISTING_STRATEGY = new Builder()
            .name("Initial Listing Strategy")
            .description("Specifies how to handle existing files on the SMB share when the processor is started for the first time (or its state has been cleared).")
            .required(true)
            .allowableValues(InitialListingStrategy.class)
            .defaultValue(InitialListingStrategy.ALL_FILES.getValue())
            .dependsOn(SMB_LISTING_STRATEGY, BY_TIMESTAMPS)
            .build();

    public static final PropertyDescriptor INITIAL_LISTING_TIMESTAMP = new Builder()
            .name("Initial Listing Timestamp")
            .description("The timestamp from which the files will be listed when the processor is started for the first time (or its state has been cleared). " +
                    "The value can be specified as an epoch timestamp in milliseconds or as a UTC datetime in a format such as 2025-02-01T00:00:00Z")
            .required(true)
            .dependsOn(INITIAL_LISTING_STRATEGY, InitialListingStrategy.FROM_TIMESTAMP)
            .addValidator(NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor SMB_CLIENT_PROVIDER_SERVICE = new Builder()
            .name("SMB Client Provider Service")
            .description("Specifies the SMB client provider to use for creating SMB connections.")
            .required(true)
            .identifiesControllerService(SmbClientProviderService.class)
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

    public static final PropertyDescriptor FILE_FILTER = new Builder()
            .name("File Filter")
            .description("Only files whose names match the given regular expression will be listed. Supports Expression Language "
                    + "against FlowFile attributes; if the resolved value is empty, no file name filter is applied.")
            .required(false)
            .addValidator(EL_AWARE_REGULAR_EXPRESSION_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor PATH_FILTER = new Builder()
            .name("Path Filter")
            .description("Only files whose paths (up to the file's parent directory) match the given regular expression will be listed. "
                    + "Supports Expression Language against FlowFile attributes; if the resolved value is empty, no path filter is applied.")
            .required(false)
            .addValidator(EL_AWARE_REGULAR_EXPRESSION_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor IGNORE_FILES_WITH_SUFFIX = new Builder()
            .name("Ignore Files with Suffix")
            .description("Files ending with the given suffix will be omitted. Can be used to make sure that files "
                    + "that are still uploading are not listed multiple times, by having those files have a suffix "
                    + "and remove the suffix once the upload finishes. This is highly recommended when using "
                    + "'Tracking Entities' or 'Tracking Timestamps' listing strategies.")
            .required(false)
            .addValidator(NON_EMPTY_VALIDATOR)
            .addValidator(new MustNotContainDirectorySeparatorsValidator())
            .build();

    public static final PropertyDescriptor TRACKING_STATE_CACHE = new Builder()
            .fromPropertyDescriptor(ListedEntityTracker.TRACKING_STATE_CACHE)
            .dependsOn(SMB_LISTING_STRATEGY, BY_ENTITIES)
            .build();

    public static final PropertyDescriptor TRACKING_TIME_WINDOW = new Builder()
            .fromPropertyDescriptor(ListedEntityTracker.TRACKING_TIME_WINDOW)
            .dependsOn(SMB_LISTING_STRATEGY, BY_ENTITIES)
            .build();

    public static final PropertyDescriptor INITIAL_LISTING_TARGET = new Builder()
            .fromPropertyDescriptor(ListedEntityTracker.INITIAL_LISTING_TARGET)
            .dependsOn(SMB_LISTING_STRATEGY, BY_ENTITIES)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SMB_CLIENT_PROVIDER_SERVICE,
            SMB_LISTING_STRATEGY,
            INITIAL_LISTING_STRATEGY,
            INITIAL_LISTING_TIMESTAMP,
            DIRECTORY,
            FILE_FILTER,
            PATH_FILTER,
            IGNORE_FILES_WITH_SUFFIX,
            AbstractListProcessor.RECORD_WRITER,
            MINIMUM_AGE,
            MAXIMUM_AGE,
            MINIMUM_SIZE,
            MAXIMUM_SIZE,
            AbstractListProcessor.TARGET_SYSTEM_TIMESTAMP_PRECISION,
            TRACKING_STATE_CACHE,
            TRACKING_TIME_WINDOW,
            INITIAL_LISTING_TARGET
    );

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
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
        return Set.of(REL_FAILURE, REL_NO_FILES, REL_SUCCESS);
    }

    private volatile Long initialListingTimestamp;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected void customValidate(ValidationContext validationContext, Collection<ValidationResult> validationResults) {
        try {
            getInitialListingTimestamp(validationContext);
        } catch (InvalidTimestampException ite) {
            validationResults.add(new ValidationResult.Builder()
                    .subject(INITIAL_LISTING_TIMESTAMP.getDisplayName())
                    .explanation(ite.getMessage())
                    .valid(false)
                    .build());
        }
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) throws IOException {
        boolean isStateEmpty = context.getStateManager().getState(getStateScope(context)).toMap().isEmpty();
        initialListingTimestamp = isStateEmpty ? getInitialListingTimestamp(context) : null;
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        super.migrateProperties(config);
        config.renameProperty(ListedEntityTracker.OLD_TRACKING_STATE_CACHE_PROPERTY_NAME, TRACKING_STATE_CACHE.getName());
        config.renameProperty(ListedEntityTracker.OLD_TRACKING_TIME_WINDOW_PROPERTY_NAME, TRACKING_TIME_WINDOW.getName());
        config.renameProperty(ListedEntityTracker.OLD_INITIAL_LISTING_TARGET_PROPERTY_NAME, INITIAL_LISTING_TARGET.getName());
        config.renameProperty("directory", DIRECTORY.getName());
        config.renameProperty("min-file-age", MINIMUM_AGE.getName());
        config.renameProperty("max-file-age", MAXIMUM_AGE.getName());
        config.renameProperty("min-file-size", MINIMUM_SIZE.getName());
        config.renameProperty("max-file-size", MAXIMUM_SIZE.getName());
        config.renameProperty("initial-listing-strategy", INITIAL_LISTING_STRATEGY.getName());
        config.renameProperty("initial-listing-timestamp", INITIAL_LISTING_TIMESTAMP.getName());
        config.renameProperty("smb-client-provider-service", SMB_CLIENT_PROVIDER_SERVICE.getName());
        config.renameProperty("file-filter", FILE_FILTER.getName());
        config.renameProperty("path-filter", PATH_FILTER.getName());
        config.renameProperty("file-name-suffix-filter", IGNORE_FILES_WITH_SUFFIX.getName());
    }

    @Override
    protected Map<String, String> createAttributes(SmbListableEntity entity, ProcessContext context) {
        final Map<String, String> attributes = new TreeMap<>();
        copyTriggerAttributes(attributes);
        final SmbClientProviderService clientProviderService =
                context.getProperty(SMB_CLIENT_PROVIDER_SERVICE).asControllerService(SmbClientProviderService.class);
        attributes.put(FILENAME, entity.getName());
        attributes.put(SHORT_NAME, entity.getShortName());
        attributes.put(PATH, entity.getPath());
        attributes.put(SERVICE_LOCATION, clientProviderService.getServiceLocation().toString());
        attributes.put(LAST_MODIFIED_TIME, formatTimeStamp(entity.getLastModifiedTime()));
        attributes.put(CREATION_TIME, formatTimeStamp(entity.getCreationTime()));
        attributes.put(LAST_ACCESS_TIME, formatTimeStamp(entity.getLastAccessTime()));
        attributes.put(CHANGE_TIME, formatTimeStamp(entity.getChangeTime()));
        attributes.put(SIZE, String.valueOf(entity.getSize()));
        attributes.put(ALLOCATION_SIZE, String.valueOf(entity.getAllocationSize()));
        return unmodifiableMap(attributes);
    }

    @Override
    protected String getPath(ProcessContext context) {
        final SmbClientProviderService clientProviderService =
                context.getProperty(SMB_CLIENT_PROVIDER_SERVICE).asControllerService(SmbClientProviderService.class);
        final URI serviceLocation = clientProviderService.getServiceLocation();
        final String directory = getDirectory(context);
        return String.format("%s/%s", serviceLocation.toString(), directory.isEmpty() ? "" : directory + "/");
    }

    @Override
    protected List<SmbListableEntity> performListing(ProcessContext context, Long minimumTimestampOrNull,
            ListingMode listingMode) throws IOException {

        final Predicate<SmbListableEntity> filePredicate = createFileFilter(context, minimumTimestampOrNull);

        try (Stream<SmbListableEntity> listing = performListing(context)) {
            final Iterator<SmbListableEntity> iterator = listing.iterator();
            final List<SmbListableEntity> result = new LinkedList<>();
            while (iterator.hasNext()) {
                if (isExecutionStopped(listingMode)) {
                    return emptyList();
                }
                final SmbListableEntity entity = iterator.next();
                if (filePredicate.test(entity)) {
                    result.add(entity);
                }
            }
            return result;
        } catch (Exception e) {
            if (listingMode == ListingMode.EXECUTION) {
                listingFailure.set(e);
            }
            throw new IOException("Could not perform listing", e);
        }
    }

    @Override
    protected boolean isListingResetNecessary(PropertyDescriptor property) {
        return asList(SMB_CLIENT_PROVIDER_SERVICE, DIRECTORY, IGNORE_FILES_WITH_SUFFIX).contains(property);
    }

    @Override
    protected Scope getStateScope(PropertyContext context) {
        return CLUSTER;
    }

    @Override
    protected RecordSchema getRecordSchema() {
        return SmbListableEntity.getRecordSchema();
    }

    @Override
    protected Integer countUnfilteredListing(ProcessContext context) throws IOException {
        try (Stream<SmbListableEntity> listing = performListing(context)) {
            return Long.valueOf(listing.count()).intValue();
        } catch (Exception e) {
            throw new IOException("Could not count files", e);
        }
    }

    @Override
    protected String getListingContainerName(ProcessContext context) {
        return String.format("Remote Directory [%s]", getPath(context));
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        FlowFile incoming = null;

        // Handle incoming FlowFiles if there are incoming connections
        if (context.hasIncomingConnection()) {
            incoming = session.get();

            // If there's no incoming FlowFile (non-loop connections), yield the processor
            if (incoming == null && context.hasNonLoopConnection()) {
                context.yield();
                return;
            }

            if (incoming != null) {
                triggerFlowAttributes.set(new HashMap<>(incoming.getAttributes()));
                // Clear the trigger from the session before listing (parent may commitAsync); EL uses the attribute snapshot.
                session.remove(incoming);
            }
        }

        try {
            SmbjClientProviderServiceExtended.setEvaluationAttributes(triggerFlowAttributes.get());

            final ProcessContext listingContext = triggerFlowAttributes.get() != null
                    ? new FlowFileAwareProcessContext(context, triggerFlowAttributes)
                    : context;
            final ProcessSession listingSession = withTriggerAttributes(session, triggerFlowAttributes.get());
            // Parent may commitAsync mid-run; trigger FlowFile must not remain in this session (see onTrigger javadoc).
            super.onTrigger(listingContext, listingSession);

            final Exception listingError = listingFailure.get();
            if (listingError != null) {
                listingFailure.remove();
                final FlowFile errorFlowFile = createErrorFlowFile(listingSession, contextForErrorAttributes(context), listingError);
                if (errorFlowFile != null) {
                    listingSession.transfer(errorFlowFile, REL_FAILURE);
                }
                return;
            }

            // Emit an empty FlowFile for empty directories if applicable
            emitEmptyFlowFileIfNeeded(listingContext, listingSession);
        } catch (final Exception e) {
            getLogger().error("Failed to perform SMB listing or handle FlowFiles due to: {}", e.getMessage(), e);

            final ProcessSession listingSession = withTriggerAttributes(session, triggerFlowAttributes.get());
            final FlowFile errorFlowFile = createErrorFlowFile(listingSession, contextForErrorAttributes(context), e);
            if (errorFlowFile != null) {
                listingSession.transfer(errorFlowFile, REL_FAILURE);
            }
        } finally {
            listingFailure.remove();
            SmbjClientProviderServiceExtended.clearEvaluationAttributes();
            triggerFlowAttributes.remove();
        }
    }

    private FlowFile createErrorFlowFile(final ProcessSession session,
                                         final ProcessContext context,
                                         final Exception error) {
        FlowFile errorFlowFile = session.create();

        // Add error-specific attributes
        final Map<String, String> attributes = new HashMap<>();
        final Throwable rootCause = getRootCause(error);
        attributes.put("error.message", rootCause.getMessage() != null ? rootCause.getMessage() : error.getMessage());
        attributes.put("error.class", rootCause.getClass().getName());
        final SmbClientProviderService clientProviderService =
                context.getProperty(SMB_CLIENT_PROVIDER_SERVICE).asControllerService(SmbClientProviderService.class);
        attributes.put("smb.service.location", clientProviderService.getServiceLocation().toString());
        attributes.put("smb.listing.directory", getDirectory(context));

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

    private ProcessContext contextForErrorAttributes(final ProcessContext context) {
        return triggerFlowAttributes.get() != null
                ? new FlowFileAwareProcessContext(context, triggerFlowAttributes)
                : context;
    }

    private static Throwable getRootCause(final Throwable throwable) {
        Throwable rootCause = throwable;
        while (rootCause.getCause() != null) {
            rootCause = rootCause.getCause();
        }
        return rootCause;
    }

    private void emitEmptyFlowFileIfNeeded(final ProcessContext context,
                                           final ProcessSession session) {

        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER)
                .asControllerService(RecordSetWriterFactory.class);

        // If no writer factory, skip processing
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

            // Populate attributes for logging/debugging
            final SmbClientProviderService clientProviderService =
                    context.getProperty(SMB_CLIENT_PROVIDER_SERVICE).asControllerService(SmbClientProviderService.class);
            attributes.put("smb.service.location", clientProviderService.getServiceLocation().toString());
            attributes.put("smb.listing.directory", getDirectory(context));

            emptyFlowFile = session.putAllAttributes(emptyFlowFile, attributes);

            session.transfer(emptyFlowFile, REL_NO_FILES);
            getLogger().debug("Emitted zero-record FlowFile to No Files because no files matched the listing filters.");
        } catch (final Exception e) {
            getLogger().error("Failed to write empty listing FlowFile due to an error.", e);
            session.remove(emptyFlowFile);
        }
    }

    /**
     * Whether the current listing configuration would yield at least one matching file (after file/path/age/size
     * filters). Exposed as a protected method so tests can stub the remote check without contacting SMB.
     */
    protected boolean hasMatchingListingResults(final ProcessContext context) throws IOException {
        final List<SmbListableEntity> listing = performListing(context, null, ListingMode.EXECUTION);
        return listing != null && !listing.isEmpty();
    }

    private String formatTimeStamp(long timestamp) {
        return ISO_DATE_TIME.format(
                LocalDateTime.ofEpochSecond(MILLISECONDS.toSeconds(timestamp), 0, UTC));
    }

    private boolean isExecutionStopped(ListingMode listingMode) {
        return ListingMode.EXECUTION.equals(listingMode) && !isScheduled();
    }

    private Predicate<SmbListableEntity> createFileFilter(ProcessContext context, Long minTimestampOrNull) {

        final Long minimumAge = context.getProperty(MINIMUM_AGE).asTimePeriod(MILLISECONDS);
        final Long maximumAgeOrNull = context.getProperty(MAXIMUM_AGE).isSet() ? context.getProperty(MAXIMUM_AGE)
                .asTimePeriod(MILLISECONDS) : null;
        final Double minimumSizeOrNull =
                context.getProperty(MINIMUM_SIZE).isSet() ? context.getProperty(MINIMUM_SIZE).asDataSize(DataUnit.B)
                        : null;
        final Double maximumSizeOrNull =
                context.getProperty(MAXIMUM_SIZE).isSet() ? context.getProperty(MAXIMUM_SIZE).asDataSize(DataUnit.B)
                        : null;
        final Map<String, String> elAttributes = triggerFlowAttributes.get() != null
                ? triggerFlowAttributes.get()
                : Collections.emptyMap();
        final Pattern filePatternOrNull = compileFilterOrNull(context.getProperty(FILE_FILTER), elAttributes);
        final Pattern pathPatternOrNull = compileFilterOrNull(context.getProperty(PATH_FILTER), elAttributes);
        final String ignoreSuffixOrNull = context.getProperty(IGNORE_FILES_WITH_SUFFIX).getValue();

        final long now = getCurrentTime();
        Predicate<SmbListableEntity> filter = entity -> now - entity.getLastModifiedTime() >= minimumAge;

        if (maximumAgeOrNull != null) {
            filter = filter.and(entity -> now - entity.getLastModifiedTime() <= maximumAgeOrNull);
        }

        if (minTimestampOrNull != null) {
            filter = filter.and(entity -> entity.getLastModifiedTime() >= minTimestampOrNull);
        }

        if (initialListingTimestamp != null) {
            filter = filter.and(entity -> entity.getLastModifiedTime() >= initialListingTimestamp);
        }

        if (minimumSizeOrNull != null) {
            filter = filter.and(entity -> entity.getSize() >= minimumSizeOrNull);
        }

        if (maximumSizeOrNull != null) {
            filter = filter.and(entity -> entity.getSize() <= maximumSizeOrNull);
        }

        if (filePatternOrNull != null) {
            filter = filter.and(entity -> filePatternOrNull.matcher(entity.getName()).matches());
        }

        if (pathPatternOrNull != null) {
            filter = filter.and(entity -> pathPatternOrNull.matcher(entity.getPath()).matches());
        }

        if (ignoreSuffixOrNull != null) {
            filter = filter.and(entity -> !entity.getName().endsWith(ignoreSuffixOrNull));
        }

        return filter;
    }

    /**
     * Resolves a regex filter property against the supplied FlowFile attributes and compiles it. Returns {@code null}
     * (meaning "no filter") when the property is unset or its resolved value is empty/blank, e.g. when an
     * Expression Language reference such as {@code ${file_filter}} resolves to an empty or missing attribute.
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

    private Stream<SmbListableEntity> performListing(ProcessContext context) throws IOException {
        final SmbClientProviderService clientProviderService =
                context.getProperty(SMB_CLIENT_PROVIDER_SERVICE).asControllerService(SmbClientProviderService.class);
        final String directory = getDirectory(context);
        final SmbClientService clientService = clientProviderService.getClient(getLogger());
        return clientService.listFiles(directory).onClose(() -> {
            try {
                clientService.close();
            } catch (Exception e) {
                throw new ProcessException("Could not close SMB client", e);
            }
        });
    }

    private String getDirectory(ProcessContext context) {
        final PropertyValue property = context.getProperty(DIRECTORY);
        final String directory = property.isSet() ? property.getValue().replace('\\', '/') : "";
        return "/".equals(directory) ? "" : directory;
    }

    private Long getInitialListingTimestamp(PropertyContext context) {
        final String listingStrategy = context.getProperty(SMB_LISTING_STRATEGY).getValue();

        if (BY_TIMESTAMPS.getValue().equals(listingStrategy)) {
            final InitialListingStrategy initialListingStrategy = context.getProperty(INITIAL_LISTING_STRATEGY).asAllowableValue(InitialListingStrategy.class);

            if (InitialListingStrategy.NEW_FILES == initialListingStrategy) {
                return Instant.now().toEpochMilli();
            } else if (InitialListingStrategy.FROM_TIMESTAMP == initialListingStrategy) {
                final String initialListingTimestamp = context.getProperty(INITIAL_LISTING_TIMESTAMP).getValue();

                try {
                    return Instant.parse(initialListingTimestamp).toEpochMilli();
                } catch (DateTimeParseException dtpe) {
                    try {
                        return Long.parseLong(initialListingTimestamp);
                    } catch (NumberFormatException nfe) {
                        throw new InvalidTimestampException(initialListingTimestamp);
                    }
                }
            }
        }

        return null;
    }

    private static class MustNotContainDirectorySeparatorsValidator implements Validator {

        @Override
        public ValidationResult validate(String subject, String value, ValidationContext context) {
            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(value)
                    .valid(!value.contains("/") && !value.contains("\\"))
                    .explanation(subject + " must not contain any folder separator character.")
                    .build();
        }

    }

    private static class InvalidTimestampException extends RuntimeException {
        InvalidTimestampException(String timestamp) {
            super(String.format("'%s' is neither an epoch timestamp nor a UTC datetime.", timestamp));
        }
    }

    /**
     * Delegates to the real {@link ProcessContext} but evaluates property expressions against the
     * trigger attribute snapshot when present (same thread as {@link ListSMBExtended#triggerFlowAttributes}).
     */
    private static class FlowFileAwareProcessContext implements ProcessContext {
        private final ProcessContext delegate;
        private final ThreadLocal<Map<String, String>> attributeSnapshot;

        FlowFileAwareProcessContext(final ProcessContext delegate, final ThreadLocal<Map<String, String>> attributeSnapshot) {
            this.delegate = delegate;
            this.attributeSnapshot = attributeSnapshot;
        }

        @Override
        public PropertyValue getProperty(final PropertyDescriptor descriptor) {
            final PropertyValue delegateValue = delegate.getProperty(descriptor);
            final Map<String, String> attrs = attributeSnapshot.get();
            if (attrs != null && !attrs.isEmpty()
                    && descriptor.getExpressionLanguageScope() == ExpressionLanguageScope.FLOWFILE_ATTRIBUTES) {
                return delegateValue.evaluateAttributeExpressions(attrs);
            }
            return delegateValue;
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
        public Map<PropertyDescriptor, String> getProperties() {
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
        public boolean hasConnection(final Relationship relationship) {
            return delegate.hasConnection(relationship);
        }

        @Override
        public boolean isExpressionLanguagePresent(final PropertyDescriptor propertyDescriptor) {
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
        public boolean isRelationshipRetried(final Relationship relationship) {
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
        public boolean isAutoTerminated(final Relationship relationship) {
            return delegate.isAutoTerminated(relationship);
        }

        @Override
        public boolean isConnectedToCluster() {
            return delegate.isConnectedToCluster();
        }
    }

}
