package org.apache.nifi.processors.mbbel;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.smb.FetchSmb;
import org.apache.nifi.processors.smb.util.CompletionStrategy;
import org.apache.nifi.services.smb.SmbClientProviderService;
import org.apache.nifi.services.smb.SmbClientService;
import org.apache.nifi.services.smb.SmbException;
import org.apache.nifi.services.smb.SmbjClientProviderServiceExtended;

import java.util.Map;
import java.util.Optional;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"samba", "smb", "cifs", "files", "fetch"})
@CapabilityDescription("Fetches files from a SMB Share. Designed to be used in tandem with ListSMBExtended. Behaves like "
        + "the standard FetchSmb processor, but publishes the incoming FlowFile's attributes to the SMB Client Provider "
        + "Service so that connection properties (Hostname, Port, Username, Password, Domain, Share) configured with "
        + "Expression Language (for example ${source_port}) are evaluated against those attributes. Use with "
        + "SmbjClientProviderServiceExtended.")
@WritesAttributes({
        @WritesAttribute(attribute = FetchSmb.ERROR_CODE_ATTRIBUTE, description = "The error code returned by SMB when the fetch of a file fails."),
        @WritesAttribute(attribute = FetchSmb.ERROR_MESSAGE_ATTRIBUTE, description = "The error message returned by SMB when the fetch of a file fails.")
})
public class FetchSMBExtended extends FetchSmb {

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
            flowFile = session.write(flowFile, outputStream -> client.readFile(filePath, outputStream));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final Exception e) {
            getLogger().error("Could not fetch file {}.", filePath, e);
            flowFile = session.putAttribute(flowFile, ERROR_CODE_ATTRIBUTE, getErrorCode(e));
            flowFile = session.putAttribute(flowFile, ERROR_MESSAGE_ATTRIBUTE, e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
            return;
        } finally {
            SmbjClientProviderServiceExtended.clearEvaluationAttributes();
        }

        session.commitAsync(() -> performCompletionStrategy(context, attributes));
    }

    private String getErrorCode(final Exception exception) {
        return Optional.ofNullable(exception instanceof SmbException ? (SmbException) exception : null)
                .map(SmbException::getErrorCode)
                .map(String::valueOf)
                .orElse(UNCATEGORIZED_ERROR);
    }

    private void performCompletionStrategy(final ProcessContext context, final Map<String, String> attributes) {
        final CompletionStrategy completionStrategy = context.getProperty(COMPLETION_STRATEGY).asAllowableValue(CompletionStrategy.class);

        if (completionStrategy == CompletionStrategy.NONE) {
            return;
        }

        final String filePath = context.getProperty(REMOTE_FILE).evaluateAttributeExpressions(attributes).getValue();

        final SmbClientProviderService clientProviderService =
                context.getProperty(SMB_CLIENT_PROVIDER_SERVICE).asControllerService(SmbClientProviderService.class);

        SmbjClientProviderServiceExtended.setEvaluationAttributes(attributes);
        try (SmbClientService client = clientProviderService.getClient(getLogger())) {
            if (completionStrategy == CompletionStrategy.MOVE) {
                final String destinationDirectory = context.getProperty(DESTINATION_DIRECTORY).evaluateAttributeExpressions(attributes).getValue();
                final boolean createDestinationDirectory = context.getProperty(CREATE_DESTINATION_DIRECTORY).asBoolean();

                if (createDestinationDirectory) {
                    client.ensureDirectory(destinationDirectory);
                }

                client.moveFile(filePath, destinationDirectory);
            } else if (completionStrategy == CompletionStrategy.DELETE) {
                client.deleteFile(filePath);
            }
        } catch (final Exception e) {
            getLogger().warn("Could not perform completion strategy {} for file {}", completionStrategy, filePath, e);
        } finally {
            SmbjClientProviderServiceExtended.clearEvaluationAttributes();
        }
    }
}
