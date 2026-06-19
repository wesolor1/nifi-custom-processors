package org.apache.nifi.processors.mbbel;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.services.smb.SmbClientProviderService;
import org.apache.nifi.services.smb.SmbClientService;
import org.apache.nifi.services.smb.SmbException;
import org.apache.nifi.services.smb.SmbjClientProviderServiceExtended;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
import static org.apache.nifi.processor.util.StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR;

@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"samba", "smb", "cifs", "files", "delete", "remove"})
@CapabilityDescription("Deletes a file residing on an SMB Share. Designed to be used in tandem with ListSMBExtended when "
        + "the file content is not needed in NiFi, avoiding the overhead of fetching the content only to delete it via a "
        + "Completion Strategy. Publishes the incoming FlowFile's attributes to the SMB Client Provider Service so that "
        + "connection properties (Hostname, Port, Username, Password, Domain, Share) configured with Expression Language "
        + "(for example ${source_port}) are evaluated against those attributes. Use with SmbjClientProviderServiceExtended.")
@SeeAlso({ListSMBExtended.class, FetchSMBExtended.class})
@WritesAttributes({
        @WritesAttribute(attribute = DeleteSmbExtended.ERROR_CODE_ATTRIBUTE, description = "The error code returned by SMB when the delete of a file fails."),
        @WritesAttribute(attribute = DeleteSmbExtended.ERROR_MESSAGE_ATTRIBUTE, description = "The error message returned by SMB when the delete of a file fails.")
})
public class DeleteSmbExtended extends AbstractProcessor {

    public static final String ERROR_CODE_ATTRIBUTE = "error.code";
    public static final String ERROR_MESSAGE_ATTRIBUTE = "error.message";

    public static final String UNCATEGORIZED_ERROR = "-2";

    // NTSTATUS codes (unsigned 32-bit) as surfaced by SMBApiException.getStatusCode() and propagated to SmbException.getErrorCode().
    static final long STATUS_NO_SUCH_FILE = 0xC000000FL;
    static final long STATUS_OBJECT_NAME_NOT_FOUND = 0xC0000034L;
    static final long STATUS_OBJECT_PATH_NOT_FOUND = 0xC000003AL;

    public static final PropertyDescriptor SMB_CLIENT_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
            .name("SMB Client Provider Service")
            .description("Specifies the SMB client provider to use for creating SMB connections.")
            .required(true)
            .identifiesControllerService(SmbClientProviderService.class)
            .build();

    public static final PropertyDescriptor REMOTE_FILE = new PropertyDescriptor.Builder()
            .name("Remote File")
            .description("The full path of the file to be deleted from the remote server. Expression language is supported.")
            .required(true)
            .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
            .defaultValue("${" + CoreAttributes.PATH.key() + "}/${" + CoreAttributes.FILENAME.key() + "}")
            .addValidator(ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile will be routed here for each file that is successfully deleted.")
            .build();

    public static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not found")
            .description("A FlowFile will be routed here when the file to delete does not exist on the remote server.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile will be routed here when an existing file could not be deleted.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_NOT_FOUND,
            REL_FAILURE
    );

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SMB_CLIENT_PROVIDER_SERVICE,
            REMOTE_FILE
    );

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final Map<String, String> attributes = flowFile.getAttributes();
        final String filePath = context.getProperty(REMOTE_FILE).evaluateAttributeExpressions(attributes).getValue();

        final SmbClientProviderService clientProviderService =
                context.getProperty(SMB_CLIENT_PROVIDER_SERVICE).asControllerService(SmbClientProviderService.class);

        SmbjClientProviderServiceExtended.setEvaluationAttributes(attributes);
        try (SmbClientService client = clientProviderService.getClient(getLogger())) {
            client.deleteFile(filePath);

            session.getProvenanceReporter().invokeRemoteProcess(flowFile, buildTransitUri(clientProviderService, filePath), "Object deleted");
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final Exception e) {
            if (isFileNotFound(e)) {
                getLogger().debug("File {} does not exist; routing to not found", filePath, e);
                session.transfer(flowFile, REL_NOT_FOUND);
            } else {
                getLogger().error("Could not delete file {}.", filePath, e);
                flowFile = session.putAttribute(flowFile, ERROR_CODE_ATTRIBUTE, getErrorCode(e));
                flowFile = session.putAttribute(flowFile, ERROR_MESSAGE_ATTRIBUTE, e.getMessage());
                session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
            }
        } finally {
            SmbjClientProviderServiceExtended.clearEvaluationAttributes();
        }
    }

    private String buildTransitUri(final SmbClientProviderService clientProviderService, final String filePath) {
        final URI serviceLocation = clientProviderService.getServiceLocation();
        final String basePath = Optional.ofNullable(serviceLocation.getPath()).orElse("");
        final String normalizedPath = normalizeRelativePath(filePath);
        final String separator = basePath.endsWith("/") || normalizedPath.isEmpty() ? "" : "/";
        final String fullPath = basePath + separator + normalizedPath;

        try {
            return new URI(serviceLocation.getScheme(), serviceLocation.getAuthority(), fullPath, null, null).toString();
        } catch (final URISyntaxException e) {
            getLogger().debug("Could not build encoded SMB provenance URI for path {}; using normalized string", filePath, e);
            final String base = serviceLocation.toString();
            return base + (base.endsWith("/") || normalizedPath.isEmpty() ? "" : "/") + normalizedPath;
        }
    }

    private String normalizeRelativePath(final String filePath) {
        final String normalizedPath = filePath.replace('\\', '/');
        return normalizedPath.startsWith("/") ? normalizedPath.substring(1) : normalizedPath;
    }

    private boolean isFileNotFound(final Exception exception) {
        if (!(exception instanceof SmbException smbException)) {
            return false;
        }
        final long errorCode = smbException.getErrorCode();
        return errorCode == STATUS_OBJECT_NAME_NOT_FOUND
                || errorCode == STATUS_OBJECT_PATH_NOT_FOUND
                || errorCode == STATUS_NO_SUCH_FILE;
    }

    private String getErrorCode(final Exception exception) {
        return Optional.ofNullable(exception instanceof SmbException ? (SmbException) exception : null)
                .map(SmbException::getErrorCode)
                .map(String::valueOf)
                .orElse(UNCATEGORIZED_ERROR);
    }
}
