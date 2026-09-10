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
package org.apache.nifi.processors.aws.credentials.provider.service;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processors.aws.credentials.provider.factory.CredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.AccessKeyPairCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.AnonymousCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.AssumeRoleCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.ExplicitDefaultCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.FileCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.ImplicitDefaultCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.NamedProfileCredentialsStrategy;
import org.apache.nifi.processors.aws.credentials.provider.factory.strategies.WebIdentityCredentialsStrategy;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Drop-in variant of {@link AWSCredentialsProviderControllerService} where {@link #ACCESS_KEY_ID Access Key ID} and
 * {@link #SECRET_KEY Secret Access Key} are not marked sensitive, so they can be set to plain text or Expression
 * Language referencing FlowFile attributes (for example {@code ${source_access_key_id}}) when used with processors
 * that publish trigger attributes via {@link #setEvaluationAttributes(Map)}.
 */
@Tags({"aws", "credentials", "provider"})
@SeeAlso({AWSCredentialsProviderControllerService.class})
@CapabilityDescription("Defines credentials for Amazon Web Services processors. Access Key ID and Secret Access Key "
        + "are not marked sensitive so they can be supplied via plain text or Expression Language against FlowFile "
        + "attributes when used with extended AWS processors.")
public class AWSCredentialsProviderControllerServiceExtended extends AWSCredentialsProviderControllerService {

    private static final ThreadLocal<Map<String, String>> EVALUATION_ATTRIBUTES = new ThreadLocal<>();

    private static final Set<String> FLOWFILE_EL_PROPERTY_NAMES = Set.of(
            AWSCredentialsProviderControllerService.ACCESS_KEY_ID.getName(),
            AWSCredentialsProviderControllerService.SECRET_KEY.getName()
    );

    /**
     * Non-sensitive variant of the stock access-key descriptor. Keeps the same name so credential strategies still
     * resolve the property; only the sensitivity flag and Expression Language scope are changed.
     */
    public static final PropertyDescriptor ACCESS_KEY_ID = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AWSCredentialsProviderControllerService.ACCESS_KEY_ID)
            .sensitive(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    /**
     * Non-sensitive variant of the stock secret-key descriptor. Keeps the same name so credential strategies still
     * resolve the property; only the sensitivity flag and Expression Language scope are changed.
     */
    public static final PropertyDescriptor SECRET_KEY = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AWSCredentialsProviderControllerService.SECRET_KEY)
            .sensitive(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private final List<CredentialsStrategy> credentialStrategies = List.of(
            new WebIdentityCredentialsStrategy(),
            new ExplicitDefaultCredentialsStrategy(),
            new AccessKeyPairCredentialsStrategy(),
            new FileCredentialsStrategy(),
            new NamedProfileCredentialsStrategy(),
            new AnonymousCredentialsStrategy(),
            new ImplicitDefaultCredentialsStrategy(),
            new AssumeRoleCredentialsStrategy()
    );

    private volatile ConfigurationContext configurationContext;

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

    @OnEnabled
    @Override
    public void onConfigured(final ConfigurationContext context) {
        super.onConfigured(context);
        this.configurationContext = context;
    }

    @OnDisabled
    @Override
    public void onDisabled() {
        super.onDisabled();
        configurationContext = null;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>(super.getSupportedPropertyDescriptors());
        for (int i = 0; i < descriptors.size(); i++) {
            final String propertyName = descriptors.get(i).getName();
            if (AWSCredentialsProviderControllerService.ACCESS_KEY_ID.getName().equals(propertyName)) {
                descriptors.set(i, ACCESS_KEY_ID);
            } else if (AWSCredentialsProviderControllerService.SECRET_KEY.getName().equals(propertyName)) {
                descriptors.set(i, SECRET_KEY);
            }
        }
        return descriptors;
    }

    @Override
    public AwsCredentialsProvider getAwsCredentialsProvider() {
        final Map<String, String> attributes = EVALUATION_ATTRIBUTES.get();
        if (attributes == null || attributes.isEmpty()) {
            return super.getAwsCredentialsProvider();
        }

        final ConfigurationContext context = configurationContext;
        if (context == null) {
            throw new IllegalStateException("AWS Credentials Provider Controller Service is not enabled");
        }

        return createCredentialsProvider(new FlowFileAttributeEvaluatingConfigurationContext(context, attributes));
    }

    private AwsCredentialsProvider createCredentialsProvider(final PropertyContext context) {
        final CredentialsStrategy primaryStrategy = selectPrimaryStrategy(context);
        final AwsCredentialsProvider primaryCredentialsProvider = primaryStrategy.getAwsCredentialsProvider(context);
        AwsCredentialsProvider derivedCredentialsProvider = null;

        for (final CredentialsStrategy strategy : credentialStrategies) {
            if (strategy.canCreateDerivedCredential(context)) {
                derivedCredentialsProvider = strategy.getDerivedAwsCredentialsProvider(context, primaryCredentialsProvider);
                break;
            }
        }

        return derivedCredentialsProvider == null ? primaryCredentialsProvider : derivedCredentialsProvider;
    }

    private CredentialsStrategy selectPrimaryStrategy(final PropertyContext propertyContext) {
        for (final CredentialsStrategy strategy : credentialStrategies) {
            if (strategy.canCreatePrimaryCredential(propertyContext)) {
                return strategy;
            }
        }
        return null;
    }

    /**
     * Delegating {@link ConfigurationContext} that resolves Expression Language on the overridden access-key properties
     * against published FlowFile attributes. All other calls pass straight through to the real context.
     */
    private static final class FlowFileAttributeEvaluatingConfigurationContext implements ConfigurationContext {
        private final ConfigurationContext delegate;
        private final Map<String, String> attributes;

        FlowFileAttributeEvaluatingConfigurationContext(final ConfigurationContext delegate, final Map<String, String> attributes) {
            this.delegate = delegate;
            this.attributes = attributes;
        }

        @Override
        public PropertyValue getProperty(final PropertyDescriptor descriptor) {
            final PropertyDescriptor resolvedDescriptor;
            if (AWSCredentialsProviderControllerService.ACCESS_KEY_ID.getName().equals(descriptor.getName())) {
                resolvedDescriptor = ACCESS_KEY_ID;
            } else if (AWSCredentialsProviderControllerService.SECRET_KEY.getName().equals(descriptor.getName())) {
                resolvedDescriptor = SECRET_KEY;
            } else {
                resolvedDescriptor = descriptor;
            }

            final PropertyValue delegateValue = delegate.getProperty(resolvedDescriptor);
            if (attributes != null && !attributes.isEmpty()
                    && FLOWFILE_EL_PROPERTY_NAMES.contains(resolvedDescriptor.getName())
                    && resolvedDescriptor.getExpressionLanguageScope() == ExpressionLanguageScope.FLOWFILE_ATTRIBUTES) {
                return delegateValue.evaluateAttributeExpressions(attributes);
            }
            return delegateValue;
        }

        @Override
        public Map<String, String> getAllProperties() {
            return delegate.getAllProperties();
        }

        @Override
        public Map<PropertyDescriptor, String> getProperties() {
            return delegate.getProperties();
        }

        @Override
        public String getAnnotationData() {
            return delegate.getAnnotationData();
        }

        @Override
        public String getSchedulingPeriod() {
            return delegate.getSchedulingPeriod();
        }

        @Override
        public Long getSchedulingPeriod(final TimeUnit timeUnit) {
            return delegate.getSchedulingPeriod(timeUnit);
        }

        @Override
        public String getName() {
            return delegate.getName();
        }
    }
}
