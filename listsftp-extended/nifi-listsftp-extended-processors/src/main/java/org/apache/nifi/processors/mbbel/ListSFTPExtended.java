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
import org.apache.nifi.logging.ComponentLog;
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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
        + "Every trigger produces exactly one outcome: listing FlowFiles on success, an empty FlowFile on No Files when nothing matched, "
        + "or an error FlowFile on Failure. A trigger is never consumed without a reply, so a scheduled batch cannot be left unanswered. "
        + "Password is non-sensitive and supports dynamic authentication: when the resolved Password, Private Key Path, or Private Key Passphrase "
        + "is blank it is treated as unset so the other credential can be used. File Filter Regex and Path Filter Regex support Expression Language "
        + "against trigger FlowFile attributes; blank resolved filter values are ignored. "
        + "When the remote listing fails (connection error, path not found, etc.), an error FlowFile "
        + "is routed to Failure with error.message / error.class and SFTP connection attributes. Incomplete recursive listings are also treated as "
        + "failures: the SFTP client logs and skips subdirectories it cannot read (read timeout, permission denied, recursion depth limit), which "
        + "would otherwise be indistinguishable from an empty directory.")
@SeeAlso({FetchSFTP.class, GetSFTP.class, PutSFTP.class})
@WritesAttributes({
        @WritesAttribute(attribute = "sftp.remote.host", description = "The hostname of the SFTP Server"),
        @WritesAttribute(attribute = "sftp.remote.port", description = "The port that was connected to on the SFTP Server"),
        @WritesAttribute(attribute = "sftp.listing.user", description = "The username of the user that performed the SFTP Listing"),
        @WritesAttribute(attribute = "sftp.remote.path", description = "The remote directory that was listed (also set on Failure FlowFiles)"),
        @WritesAttribute(attribute = "error.message", description = "Root-cause message when listing fails (Failure relationship)"),
        @WritesAttribute(attribute = "error.class", description = "Root-cause exception class when listing fails (Failure relationship)"),
        @WritesAttribute(attribute = "sftp.listing.incomplete.directories",
                description = "Number of directories that could not be listed during a recursive listing (Failure relationship)"),
        @WritesAttribute(attribute = "record.count", description = "Number of listed files; 0 on the No Files relationship"),
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

    /**
     * Set when {@link #performListing} fails during execution. The parent {@code AbstractListProcessor}
     * swallows {@link IOException} (logs + yields), so Failure routing is detected via this ThreadLocal
     * rather than the {@code onTrigger} catch block alone.
     */
    private final ThreadLocal<Exception> listingFailure = new ThreadLocal<>();

    /**
     * Number of files returned by the last EXECUTION listing, or {@code null} when the listing never ran. Lets an
     * empty remote directory (No Files) be told apart from a listing that found files but produced no FlowFile
     * (Failure), without listing the remote system a second time.
     */
    private final ThreadLocal<Integer> executionListingCount = new ThreadLocal<>();

    /**
     * Directories that could not be listed during a recursive listing. {@code SFTPTransfer} logs and skips them
     * instead of throwing, so without this a read timeout deep in the tree looks exactly like an empty directory.
     * Populated through {@link #getListingLogger()}.
     */
    private final ThreadLocal<List<String>> incompleteListingDetails = new ThreadLocal<>();

    private volatile Predicate<FileInfo> fileFilter;

    /** Logged by {@code SFTPTransfer} for each subdirectory it fails to list, then swallowed. */
    private static final String DIRECTORY_LISTING_FAILURE_MESSAGE = "Unable to get listing from";

    /** Logged by {@code SFTPTransfer} when it abandons recursion, which silently truncates the listing. */
    private static final String RECURSION_LIMIT_MESSAGE = "had to stop recursively searching directories";

    static final String INCOMPLETE_DIRECTORIES_ATTRIBUTE = "sftp.listing.incomplete.directories";

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
            .description("An error FlowFile is routed here when the remote listing could not be performed "
                    + "(e.g. connection failure, authentication error, or remote path not found).")
            .autoTerminateDefault(true)
            .build();

    public static final Relationship REL_NO_FILES = new Relationship.Builder()
            .name("No Files")
            .description("An empty FlowFile when the listing yields no matching files after filters are applied "
                    + "(including an empty remote directory). Zero-record content is written when a Record Writer is configured.")
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
        return new SFTPTransfer(wrappedContext, getListingLogger());
    }

    /**
     * Logger handed to {@link SFTPTransfer}. Behaves like {@link #getLogger()} but also records the per-directory
     * failures that {@code SFTPTransfer} logs and swallows during a recursive listing, so a partially completed
     * listing can be routed to {@link #REL_FAILURE} instead of being reported as an empty directory.
     */
    protected ComponentLog getListingLogger() {
        final ComponentLog delegate = getLogger();

        final InvocationHandler handler = (proxy, method, args) -> {
            final String methodName = method.getName();
            if (("error".equals(methodName) || "warn".equals(methodName))
                    && args != null && args.length > 0 && args[0] instanceof String message
                    && (message.contains(DIRECTORY_LISTING_FAILURE_MESSAGE) || message.contains(RECURSION_LIMIT_MESSAGE))) {
                recordIncompleteListing(message, args);
            }
            return invokeUnwrapped(delegate, method, args);
        };

        return (ComponentLog) Proxy.newProxyInstance(
                ComponentLog.class.getClassLoader(),
                new Class<?>[] {ComponentLog.class},
                handler);
    }

    private void recordIncompleteListing(final String message, final Object[] args) {
        final List<String> details = incompleteListingDetails.get();
        if (details == null) {
            // Logged outside onTrigger (e.g. configuration verification); there is no listing to correlate it with.
            return;
        }

        final StringBuilder detail = new StringBuilder(message);
        appendLogArguments(detail, Arrays.copyOfRange(args, 1, args.length));
        details.add(detail.toString());
    }

    private static void appendLogArguments(final StringBuilder detail, final Object[] args) {
        for (final Object arg : args) {
            if (arg instanceof Object[] nested) {
                appendLogArguments(detail, nested);
            } else if (arg instanceof Throwable throwable) {
                final Throwable rootCause = getRootCause(throwable);
                detail.append(" [")
                        .append(rootCause.getClass().getSimpleName())
                        .append(": ")
                        .append(rootCause.getMessage())
                        .append(']');
            } else if (arg != null) {
                detail.append(' ').append(arg);
            }
        }
    }

    private List<String> getIncompleteListingDetails() {
        final List<String> details = incompleteListingDetails.get();
        return details == null ? List.of() : details;
    }

    private static Object invokeUnwrapped(final Object target, final Method method, final Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (final InvocationTargetException e) {
            throw e.getCause() == null ? e : e.getCause();
        }
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

    /**
     * The parent routes every listing through this overload, making it the one place where the number of listed
     * files can be recorded even when {@link #performListing(ProcessContext, Long, ListingMode, boolean)} is
     * overridden.
     */
    @Override
    protected List<FileInfo> performListing(final ProcessContext context, final Long minTimestamp, final ListingMode listingMode)
            throws IOException {
        final List<FileInfo> listing = super.performListing(context, minTimestamp, listingMode);

        if (listingMode == ListingMode.EXECUTION) {
            executionListingCount.set((int) listing.stream().filter(info -> !info.isDirectory()).count());
        }

        return listing;
    }

    @Override
    protected List<FileInfo> performListing(final ProcessContext context, final Long minTimestamp, final ListingMode listingMode,
                                            final boolean applyFilters) throws IOException {
        try {
            final List<FileInfo> listing = super.performListing(context, minTimestamp, listingMode, applyFilters);

            if (!applyFilters) {
                return listing;
            }

            final Predicate<FileInfo> filePredicate = listingMode == ListingMode.EXECUTION ? this.fileFilter : createFileFilter(context);
            return listing.stream()
                    .filter(filePredicate)
                    .collect(Collectors.toList());
        } catch (final Exception e) {
            if (listingMode == ListingMode.EXECUTION) {
                listingFailure.set(e);
            }
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            throw new IOException("Could not perform listing", e);
        }
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

        incompleteListingDetails.set(new ArrayList<>());
        executionListingCount.remove();

        final AtomicInteger listedFlowFileCount = new AtomicInteger();
        final ProcessSession listingSession = wrapListingSession(session, triggerFlowAttributes.get(), listedFlowFileCount);
        final long startedNanos = System.nanoTime();

        try {
            // Parent may swallow listing IOExceptions (log + yield); Failure is detected via listingFailure.
            super.onTrigger(context, listingSession);

            final Exception listingError = listingFailure.get();
            if (listingError != null) {
                transferErrorFlowFile(listingSession, context, listingError);
                logListingOutcome(context, REL_FAILURE.getName(), listedFlowFileCount.get(), startedNanos);
                return;
            }

            if (listedFlowFileCount.get() > 0) {
                if (!getIncompleteListingDetails().isEmpty()) {
                    getLogger().warn("SFTP listing of {} returned {} FlowFile(s) but skipped {} unreadable directory(ies); "
                                    + "files below them are not listed in this run. First skipped: {}",
                            resolveProperty(context, REMOTE_PATH), listedFlowFileCount.get(),
                            getIncompleteListingDetails().size(), getIncompleteListingDetails().get(0));
                }
                logListingOutcome(context, REL_SUCCESS.getName(), listedFlowFileCount.get(), startedNanos);
                return;
            }

            // Nothing was transferred. The parent yields silently in several cases (empty listing, Record Writer
            // failure, state reset failure), and the trigger FlowFile is already gone, so decide here rather than
            // leaving the caller without an answer.
            final Exception unexplainedEmptyListing = findEmptyListingFailure(context);
            if (unexplainedEmptyListing != null) {
                transferErrorFlowFile(listingSession, context, unexplainedEmptyListing);
                logListingOutcome(context, REL_FAILURE.getName(), 0, startedNanos);
                return;
            }

            logListingOutcome(context, transferNoFilesFlowFile(context, listingSession), 0, startedNanos);
        } catch (final Exception e) {
            getLogger().error("Failed to perform SFTP listing or handle FlowFiles due to: {}", e.getMessage(), e);
            transferErrorFlowFile(listingSession, context, e);
            logListingOutcome(context, REL_FAILURE.getName(), listedFlowFileCount.get(), startedNanos);
        } finally {
            listingFailure.remove();
            triggerFlowAttributes.remove();
            executionListingCount.remove();
            incompleteListingDetails.remove();
        }
    }

    /**
     * Why an execution produced no FlowFile at all, or {@code null} when the remote directory genuinely held no
     * matching file and No Files is the correct answer.
     */
    private Exception findEmptyListingFailure(final ProcessContext context) {
        final List<String> incompleteDirectories = getIncompleteListingDetails();
        if (!incompleteDirectories.isEmpty()) {
            return new IOException(String.format(
                    "Listing was incomplete: %d director(ies) could not be read, so an empty result cannot be trusted. "
                            + "First failure: %s. Check remote permissions and raise Data Timeout if the tree is large.",
                    incompleteDirectories.size(), incompleteDirectories.get(0)));
        }

        final Integer listedFiles = executionListingCount.get();
        if (listedFiles == null) {
            return new IOException("Listing did not run: the processor produced no FlowFile and never reached the remote "
                    + "system. Check the preceding log entries for state or configuration errors.");
        }

        final boolean noTracking = NO_TRACKING.getValue()
                .equals(context.getProperty(FILE_TRANSFER_LISTING_STRATEGY).getValue());
        if (listedFiles > 0 && noTracking) {
            return new IOException(String.format(
                    "Listing matched %d file(s) but no listing FlowFile was produced; writing the listing failed. "
                            + "Check the preceding 'Failed to write listing to FlowFile' log entry and the Record Writer.",
                    listedFiles));
        }

        return null;
    }

    private void transferErrorFlowFile(final ProcessSession session, final ProcessContext context, final Exception error) {
        listingFailure.remove();

        final FlowFile errorFlowFile = createErrorFlowFile(session, context, error);
        if (errorFlowFile != null) {
            session.transfer(errorFlowFile, REL_FAILURE);
        }
    }

    /**
     * One line describing what the execution did. Only interesting per execution while troubleshooting, so it stays on
     * DEBUG; a failed execution logs it on WARN because that is exactly when the listing context is needed.
     */
    private void logListingOutcome(final ProcessContext context, final String relationship,
                                   final int flowFileCount, final long startedNanos) {
        final boolean failed = REL_FAILURE.getName().equals(relationship);
        if (!failed && !getLogger().isDebugEnabled()) {
            return;
        }

        final Integer listedFiles = executionListingCount.get();
        final String message = "SFTP listing finished: relationship={} flowFiles={} listedFiles={} "
                + "incompleteDirectories={} host={} remotePath={} fileFilter={} pathFilter={} durationMs={}";
        final Object[] details = {
                relationship, flowFileCount, listedFiles == null ? "n/a" : listedFiles,
                getIncompleteListingDetails().size(), resolveProperty(context, SFTPTransfer.HOSTNAME),
                resolveProperty(context, REMOTE_PATH), resolveProperty(context, FILE_FILTER_REGEX),
                resolveProperty(context, PATH_FILTER_REGEX),
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos)
        };

        if (failed) {
            getLogger().warn(message, details);
        } else {
            getLogger().debug(message, details);
        }
    }

    /** EL-resolved property value for logging and FlowFile attributes; never throws. */
    private String resolveProperty(final ProcessContext context, final PropertyDescriptor descriptor) {
        final Map<String, String> elAttributes = triggerFlowAttributes.get() != null ? triggerFlowAttributes.get() : Map.of();
        try {
            return context.getProperty(descriptor).evaluateAttributeExpressions(elAttributes).getValue();
        } catch (final Exception e) {
            getLogger().debug("Could not resolve {}: {}", descriptor.getName(), e.getMessage());
            return null;
        }
    }

    private FlowFile createErrorFlowFile(final ProcessSession session,
                                         final ProcessContext context,
                                         final Exception error) {
        FlowFile errorFlowFile = session.create();

        final Map<String, String> attributes = new HashMap<>();
        final Throwable rootCause = getRootCause(error);
        attributes.put("error.message", rootCause.getMessage() != null ? rootCause.getMessage() : error.getMessage());
        attributes.put("error.class", rootCause.getClass().getName());
        attributes.put(INCOMPLETE_DIRECTORIES_ATTRIBUTE, String.valueOf(getIncompleteListingDetails().size()));
        putConnectionAttributes(attributes, context);
        putIfResolved(attributes, "sftp.remote.path", resolveProperty(context, REMOTE_PATH));

        errorFlowFile = session.putAllAttributes(errorFlowFile, attributes);

        getLogger().debug("Created error FlowFile with attributes: {}", attributes);

        return errorFlowFile;
    }

    private void putConnectionAttributes(final Map<String, String> attributes, final ProcessContext context) {
        putIfResolved(attributes, "sftp.remote.host", resolveProperty(context, SFTPTransfer.HOSTNAME));
        putIfResolved(attributes, "sftp.remote.port", resolveProperty(context, SFTPTransfer.PORT));
        putIfResolved(attributes, "sftp.listing.user", resolveProperty(context, SFTPTransfer.USERNAME));
    }

    private static void putIfResolved(final Map<String, String> attributes, final String name, final String value) {
        if (value != null) {
            attributes.put(name, value);
        }
    }

    private static Throwable getRootCause(final Throwable throwable) {
        Throwable rootCause = throwable;
        while (rootCause.getCause() != null) {
            rootCause = rootCause.getCause();
        }
        return rootCause;
    }

    private void copyTriggerAttributes(final Map<String, String> attributes) {
        final Map<String, String> triggerAttrs = triggerFlowAttributes.get();
        if (triggerAttrs != null && !triggerAttrs.isEmpty()) {
            attributes.putAll(triggerAttrs);
        }
    }

    /**
     * Session view used for the listing itself: it copies the trigger attributes onto every emitted FlowFile and
     * counts what the parent transfers to {@code success}, which is how an execution that produced nothing is
     * detected without listing the remote system again.
     */
    private static ProcessSession wrapListingSession(final ProcessSession session,
                                                     final Map<String, String> triggerAttributes,
                                                     final AtomicInteger successCount) {
        final boolean mergeAttributes = triggerAttributes != null && !triggerAttributes.isEmpty();

        final InvocationHandler handler = new InvocationHandler() {
            @Override
            public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
                final String methodName = method.getName();

                if (mergeAttributes && "putAllAttributes".equals(methodName)
                        && args != null && args.length == 2 && args[1] instanceof Map) {
                    @SuppressWarnings("unchecked")
                    final Map<String, String> merged = new HashMap<>(triggerAttributes);
                    merged.putAll((Map<String, String>) args[1]);
                    args[1] = merged;
                } else if ("transfer".equals(methodName) && args != null && args.length == 2
                        && REL_SUCCESS.equals(args[1])) {
                    successCount.addAndGet(args[0] instanceof Collection<?> batch ? batch.size() : 1);
                }

                return invokeUnwrapped(session, method, args);
            }
        };

        return (ProcessSession) Proxy.newProxyInstance(
                ProcessSession.class.getClassLoader(),
                new Class<?>[] {ProcessSession.class},
                handler);
    }

    /** @return the relationship the FlowFile was routed to, for the outcome log entry. */
    private String transferNoFilesFlowFile(final ProcessContext context, final ProcessSession session) {
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER)
                .asControllerService(RecordSetWriterFactory.class);

        FlowFile emptyFlowFile = session.create();
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("record.count", "0");

        try {
            if (writerFactory == null) {
                getLogger().debug("No Record Writer configured; emitting an empty FlowFile to {} so the trigger is "
                        + "always answered.", REL_NO_FILES.getName());
            } else {
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
            }

            putConnectionAttributes(attributes, context);
            emptyFlowFile = session.putAllAttributes(emptyFlowFile, attributes);

            session.transfer(emptyFlowFile, REL_NO_FILES);
            return REL_NO_FILES.getName();
        } catch (final Exception e) {
            getLogger().error("Failed to write the empty listing FlowFile; routing to {} instead.", REL_FAILURE.getName(), e);
            session.remove(emptyFlowFile);
            transferErrorFlowFile(session, context, e);
            return REL_FAILURE.getName();
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
