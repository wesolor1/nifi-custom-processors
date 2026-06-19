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
package org.apache.nifi.services.azure.storage;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.services.azure.AzureIdentityFederationTokenProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.CREDENTIALS_TYPE;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.MANAGED_IDENTITY_CLIENT_ID;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.SERVICE_PRINCIPAL_CLIENT_ID;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.SERVICE_PRINCIPAL_CLIENT_SECRET;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.SERVICE_PRINCIPAL_TENANT_ID;

/**
 * Drop-in variant of {@link ADLSCredentialsControllerService} where {@link #ACCOUNT_NAME Storage Account Name} is not
 * marked sensitive, so it can be set to plain text or Expression Language referencing FlowFile attributes (for example
 * {@code ${source_storage_account}}) when used with processors that pass trigger attributes to
 * {@link ADLSCredentialsService#getCredentialsDetails(Map)}.
 */
@Tags({"azure", "microsoft", "cloud", "storage", "adls", "credentials"})
@SeeAlso({ADLSCredentialsControllerService.class})
@CapabilityDescription("Defines credentials for ADLS processors. Storage Account Name is not marked sensitive so it "
        + "can be supplied via Expression Language against FlowFile attributes when used with extended ADLS processors.")
public class ADLSCredentialsControllerServiceExtended extends ADLSCredentialsControllerService {

    /**
     * Non-sensitive variant of the stock account-name descriptor. Keeps the same name and Expression Language scope so
     * configured values still resolve when {@code getCredentialsDetails} reads the property; only the sensitivity flag
     * is flipped to {@code false}.
     */
    public static final PropertyDescriptor ACCOUNT_NAME = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(ADLSCredentialsControllerService.ACCOUNT_NAME)
            .sensitive(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private volatile ConfigurationContext configurationContext;

    @OnEnabled
    @Override
    public void onEnabled(final ConfigurationContext context) {
        super.onEnabled(context);
        this.configurationContext = context;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>(super.getSupportedPropertyDescriptors());
        for (int i = 0; i < descriptors.size(); i++) {
            if (descriptors.get(i).getName().equals(ADLSCredentialsControllerService.ACCOUNT_NAME.getName())) {
                descriptors.set(i, ACCOUNT_NAME);
                break;
            }
        }
        return descriptors;
    }

    @Override
    public ADLSCredentialsDetails getCredentialsDetails(final Map<String, String> attributes) {
        final ConfigurationContext context = configurationContext;
        if (context == null) {
            throw new IllegalStateException("ADLS Credentials Controller Service is not enabled");
        }

        final ADLSCredentialsDetails.Builder credentialsBuilder = ADLSCredentialsDetails.Builder.newBuilder();

        setValue(context, credentialsBuilder, ACCOUNT_NAME, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setAccountName, attributes);
        setValue(context, credentialsBuilder, ACCOUNT_KEY, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setAccountKey, attributes);
        setValue(context, credentialsBuilder, SAS_TOKEN, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setSasToken, attributes);
        setValue(context, credentialsBuilder, ENDPOINT_SUFFIX, PropertyValue::getValue, ADLSCredentialsDetails.Builder::setEndpointSuffix, attributes);
        setValue(context, credentialsBuilder, CREDENTIALS_TYPE,
                property -> property.asAllowableValue(AzureStorageCredentialsType.class) == AzureStorageCredentialsType.MANAGED_IDENTITY,
                ADLSCredentialsDetails.Builder::setUseManagedIdentity, attributes);
        setValue(context, credentialsBuilder, MANAGED_IDENTITY_CLIENT_ID, PropertyValue::getValue,
                ADLSCredentialsDetails.Builder::setManagedIdentityClientId, attributes);
        setValue(context, credentialsBuilder, SERVICE_PRINCIPAL_TENANT_ID, PropertyValue::getValue,
                ADLSCredentialsDetails.Builder::setServicePrincipalTenantId, attributes);
        setValue(context, credentialsBuilder, SERVICE_PRINCIPAL_CLIENT_ID, PropertyValue::getValue,
                ADLSCredentialsDetails.Builder::setServicePrincipalClientId, attributes);
        setValue(context, credentialsBuilder, SERVICE_PRINCIPAL_CLIENT_SECRET, PropertyValue::getValue,
                ADLSCredentialsDetails.Builder::setServicePrincipalClientSecret, attributes);

        if (context.getProperty(CREDENTIALS_TYPE).asAllowableValue(AzureStorageCredentialsType.class) == AzureStorageCredentialsType.IDENTITY_FEDERATION) {
            final AzureIdentityFederationTokenProvider identityTokenProvider = context.getProperty(AzureStorageUtils.IDENTITY_FEDERATION_TOKEN_PROVIDER)
                    .asControllerService(AzureIdentityFederationTokenProvider.class);
            credentialsBuilder.setIdentityTokenProvider(identityTokenProvider);
        }

        credentialsBuilder.setProxyOptions(AzureStorageUtils.getProxyOptions(context));

        return credentialsBuilder.build();
    }

    private <T> void setValue(
            final ConfigurationContext context,
            final ADLSCredentialsDetails.Builder credentialsBuilder,
            final PropertyDescriptor propertyDescriptor,
            final Function<PropertyValue, T> getPropertyValue,
            final BiConsumer<ADLSCredentialsDetails.Builder, T> setBuilderValue,
            final Map<String, String> attributes) {
        PropertyValue property = context.getProperty(propertyDescriptor);

        if (property.isSet()) {
            if (propertyDescriptor.isExpressionLanguageSupported()) {
                if (propertyDescriptor.getExpressionLanguageScope() == ExpressionLanguageScope.FLOWFILE_ATTRIBUTES) {
                    property = property.evaluateAttributeExpressions(attributes);
                } else {
                    property = property.evaluateAttributeExpressions();
                }
            }
            final T value = getPropertyValue.apply(property);
            setBuilderValue.accept(credentialsBuilder, value);
        }
    }
}
