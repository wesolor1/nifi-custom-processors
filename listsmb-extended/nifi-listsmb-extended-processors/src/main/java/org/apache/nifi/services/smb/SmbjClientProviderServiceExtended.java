package org.apache.nifi.services.smb;

import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.Share;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.smb.common.SmbUtils;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.processor.util.StandardValidators.NON_BLANK_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.NON_EMPTY_VALIDATOR;
import static org.apache.nifi.processor.util.StandardValidators.PORT_VALIDATOR;
import static org.apache.nifi.smb.common.SmbProperties.ENABLE_DFS;
import static org.apache.nifi.smb.common.SmbProperties.OLD_ENABLE_DFS_PROPERTY_NAME;
import static org.apache.nifi.smb.common.SmbProperties.OLD_SMB_DIALECT_PROPERTY_NAME;
import static org.apache.nifi.smb.common.SmbProperties.OLD_TIMEOUT_PROPERTY_NAME;
import static org.apache.nifi.smb.common.SmbProperties.OLD_USE_ENCRYPTION_PROPERTY_NAME;
import static org.apache.nifi.smb.common.SmbProperties.SMB_DIALECT;
import static org.apache.nifi.smb.common.SmbProperties.TIMEOUT;
import static org.apache.nifi.smb.common.SmbProperties.USE_ENCRYPTION;

@Tags({"samba", "smb", "cifs", "files"})
@CapabilityDescription("Provides access to SMB sessions with shared authentication credentials. Connection properties "
        + "support Expression Language evaluated against FlowFile attributes when used with processors that publish "
        + "trigger attributes (for example ListSMBExtended).")
public class SmbjClientProviderServiceExtended extends AbstractControllerService implements SmbClientProviderService {

    private static final ThreadLocal<Map<String, String>> EVALUATION_ATTRIBUTES = new ThreadLocal<>();

    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("Hostname")
            .description("The network host of the SMB file server.")
            .required(true)
            .addValidator(NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor DOMAIN = new PropertyDescriptor.Builder()
            .name("Domain")
            .description("The domain used for authentication. Optional, in most cases username and password is sufficient.")
            .required(false)
            .addValidator(NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("The username used for authentication.")
            .required(false)
            .defaultValue("Guest")
            .addValidator(NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("The password used for authentication.")
            .required(false)
            .addValidator(NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("Port")
            .description("Port to use for connection.")
            .required(true)
            .addValidator(PORT_VALIDATOR)
            .defaultValue("445")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor SHARE = new PropertyDescriptor.Builder()
            .name("Share")
            .description("The network share to which files should be listed from. This is the \"first folder\" "
                    + "after the hostname: smb://hostname:port/[share]/dir1/dir2")
            .required(true)
            .addValidator(NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            HOSTNAME,
            PORT,
            SHARE,
            USERNAME,
            PASSWORD,
            DOMAIN,
            SMB_DIALECT,
            USE_ENCRYPTION,
            ENABLE_DFS,
            TIMEOUT
    );

    private volatile ConfigurationContext configurationContext;
    private volatile SMBClient smbClient;

    public static void setEvaluationAttributes(final Map<String, String> attributes) {
        if (attributes == null || attributes.isEmpty()) {
            EVALUATION_ATTRIBUTES.remove();
        } else {
            EVALUATION_ATTRIBUTES.set(attributes);
        }
    }

    public static void clearEvaluationAttributes() {
        EVALUATION_ATTRIBUTES.remove();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        configurationContext = context;
        smbClient = SmbUtils.buildSmbClient(context);
    }

    @OnDisabled
    public void onDisabled() {
        if (smbClient != null) {
            smbClient.close();
            smbClient = null;
        }
        configurationContext = null;
    }

    @Override
    public URI getServiceLocation() {
        final ResolvedConnection connection = resolveConnection();
        return URI.create(String.format("smb://%s:%d/%s", connection.hostname(), connection.port(), connection.shareName()));
    }

    @Override
    public SmbClientService getClient(final ComponentLog logger) throws IOException {
        final ResolvedConnection connection = resolveConnection();
        final Connection smbConnection = smbClient.connect(connection.hostname(), connection.port());

        final Session session;
        final Share share;

        try {
            session = smbConnection.authenticate(connection.authenticationContext());
        } catch (final Exception e) {
            throw new IOException("Could not create session for share " + getServiceLocation(), e);
        }

        try {
            share = session.connectShare(connection.shareName());
        } catch (final Exception e) {
            closeSession(session);
            throw new IOException("Could not connect to share " + getServiceLocation(), e);
        }

        if (!(share instanceof DiskShare)) {
            closeSession(session);
            throw new IllegalArgumentException("DiskShare not found. Share " + share.getClass().getSimpleName()
                    + " found on " + getServiceLocation());
        }

        return new SmbjClientService(session, (DiskShare) share, getServiceLocation(), logger);
    }

    @Override
    public void migrateProperties(final PropertyConfiguration config) {
        config.renameProperty("hostname", HOSTNAME.getName());
        config.renameProperty("domain", DOMAIN.getName());
        config.renameProperty("username", USERNAME.getName());
        config.renameProperty("password", PASSWORD.getName());
        config.renameProperty("port", PORT.getName());
        config.renameProperty("share", SHARE.getName());
        config.renameProperty(OLD_ENABLE_DFS_PROPERTY_NAME, ENABLE_DFS.getName());
        config.renameProperty(OLD_SMB_DIALECT_PROPERTY_NAME, SMB_DIALECT.getName());
        config.renameProperty(OLD_TIMEOUT_PROPERTY_NAME, TIMEOUT.getName());
        config.renameProperty(OLD_USE_ENCRYPTION_PROPERTY_NAME, USE_ENCRYPTION.getName());
    }

    private ResolvedConnection resolveConnection() {
        final ConfigurationContext context = configurationContext;
        if (context == null) {
            throw new IllegalStateException("SMB Client Provider Service is not enabled");
        }

        final String hostname = evaluateProperty(context, HOSTNAME).getValue();
        final int port = evaluateProperty(context, PORT).asInteger();
        final String shareName = evaluateProperty(context, SHARE).getValue();
        final AuthenticationContext authenticationContext = createAuthenticationContext(context);
        return new ResolvedConnection(hostname, port, shareName, authenticationContext);
    }

    private PropertyValue evaluateProperty(final ConfigurationContext context, final PropertyDescriptor descriptor) {
        final PropertyValue value = context.getProperty(descriptor);
        final Map<String, String> attributes = EVALUATION_ATTRIBUTES.get();
        if (attributes != null && !attributes.isEmpty()
                && descriptor.getExpressionLanguageScope() == ExpressionLanguageScope.FLOWFILE_ATTRIBUTES) {
            return value.evaluateAttributeExpressions(attributes);
        }
        return value;
    }

    private AuthenticationContext createAuthenticationContext(final ConfigurationContext context) {
        final PropertyValue username = evaluateProperty(context, USERNAME);
        if (username.isSet()) {
            final PropertyValue passwordValue = evaluateProperty(context, PASSWORD);
            final String password = passwordValue.isSet() ? passwordValue.getValue() : "";
            final PropertyValue domain = evaluateProperty(context, DOMAIN);
            final String domainOrNull = domain.isSet() ? domain.getValue() : null;
            return new AuthenticationContext(username.getValue(), password.toCharArray(), domainOrNull);
        }
        return AuthenticationContext.anonymous();
    }

    private void closeSession(final Session session) {
        try {
            if (session != null) {
                session.close();
            }
        } catch (final Exception e) {
            getLogger().error("Could not close session to {}", getServiceLocation(), e);
        }
    }

    private record ResolvedConnection(String hostname, int port, String shareName, AuthenticationContext authenticationContext) {
    }
}
