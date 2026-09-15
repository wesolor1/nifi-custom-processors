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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerServiceExtended;
import org.apache.nifi.processors.aws.s3.PutS3Object;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.builder.SdkClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.List;
import java.util.Map;

/**
 * Drop-in variant of {@link PutS3Object} for use with {@link ListS3Extended} and
 * {@link AWSCredentialsProviderControllerServiceExtended}. Incoming FlowFile attributes are published so Access Key ID
 * and Secret Access Key configured with Expression Language (for example {@code ${source_access_key_id}}) are evaluated
 * against those attributes. The credentials provider re-resolves on each SDK request so a cached S3 client still picks
 * up per-FlowFile credentials.
 */
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"Amazon", "S3", "AWS", "Archive", "Put"})
@CapabilityDescription("Puts FlowFiles to an Amazon S3 Bucket. This is a drop-in variant of PutS3Object designed for "
        + "use with ListS3Extended and AWSCredentialsProviderControllerServiceExtended, where Access Key ID, Secret "
        + "Access Key and Endpoint Override URL can be supplied via Expression Language against incoming FlowFile "
        + "attributes (for example ${source_access_key_id}, ${source_endpoint_override}). Incoming FlowFile attributes "
        + "are published to the credentials service before the upload so they are available when credentials are resolved. "
        + "All other behavior is identical to the standard PutS3Object processor.")
@SeeAlso({PutS3Object.class, ListS3Extended.class, FetchS3ObjectExtended.class, DeleteS3ObjectExtended.class,
        AWSCredentialsProviderControllerServiceExtended.class})
public class PutS3ObjectExtended extends PutS3Object {

    /**
     * Snapshot of the current FlowFile's attributes, used when {@link AWSCredentialsProviderControllerServiceExtended}
     * resolves Access Key ID / Secret Access Key. Always cleared in a {@code finally} block around {@link #onTrigger}.
     */
    private final ThreadLocal<Map<String, String>> triggerFlowAttributes = new ThreadLocal<>();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return S3FlowFileEndpointSupport.withFlowFileEndpoint(super.getSupportedPropertyDescriptors());
    }

    /**
     * Skip {@code AbstractAwsProcessor}'s eager S3 client so Endpoint Override Expression Language is not baked in
     * as empty before the first FlowFile arrives.
     */
    @Override
    public void onScheduled(final ProcessContext context) {
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        try {
            super.onTrigger(context, session);
        } finally {
            triggerFlowAttributes.remove();
            AWSCredentialsProviderControllerServiceExtended.clearEvaluationAttributes();
        }
    }

    /**
     * Stock PutS3Object calls this with {@code flowFile.getAttributes()} before talking to S3. Capture those attributes
     * so credential Expression Language can be evaluated against the same FlowFile.
     */
    @Override
    protected S3Client getClient(final ProcessContext context, final Map<String, String> attributes) {
        captureTriggerAttributes(attributes);
        return super.getClient(context, attributes);
    }

    @Override
    @SuppressWarnings("rawtypes")
    protected void configureEndpoint(final ProcessContext context, final SdkClientBuilder clientBuilder,
                                     final PropertyDescriptor endpointOverrideDescriptor) {
        final Map<String, String> attributes = triggerFlowAttributes.get() != null ? triggerFlowAttributes.get() : Map.of();
        S3FlowFileEndpointSupport.apply(context, clientBuilder, attributes, getLogger());
    }

    /**
     * Records the current FlowFile attributes for credential resolution. Exposed so tests can exercise the publishing
     * path without constructing an AWS SDK client.
     */
    void captureTriggerAttributes(final Map<String, String> attributes) {
        if (attributes != null && !attributes.isEmpty()) {
            triggerFlowAttributes.set(attributes);
        }
    }

    /**
     * Returns a provider that re-resolves credentials on every SDK call. AbstractAwsProcessor caches S3 clients by
     * region (including an eager client created in {@code onScheduled}), so a static snapshot taken at client-build
     * time would ignore later FlowFile attributes.
     */
    @Override
    protected AwsCredentialsProvider getCredentialsProvider(final ProcessContext context) {
        return new FlowFileAwareCredentialsProvider(context);
    }

    private final class FlowFileAwareCredentialsProvider implements AwsCredentialsProvider {
        private final ProcessContext context;

        private FlowFileAwareCredentialsProvider(final ProcessContext context) {
            this.context = context;
        }

        @Override
        public AwsCredentials resolveCredentials() {
            final Map<String, String> attributes = triggerFlowAttributes.get();
            if (attributes != null && !attributes.isEmpty()) {
                AWSCredentialsProviderControllerServiceExtended.setEvaluationAttributes(attributes);
            }
            try {
                return PutS3ObjectExtended.super.getCredentialsProvider(context).resolveCredentials();
            } finally {
                AWSCredentialsProviderControllerServiceExtended.clearEvaluationAttributes();
            }
        }
    }
}
