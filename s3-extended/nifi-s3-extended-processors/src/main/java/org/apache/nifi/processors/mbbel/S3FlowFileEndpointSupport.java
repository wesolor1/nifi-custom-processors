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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.AbstractAwsProcessor;
import software.amazon.awssdk.core.client.builder.SdkClientBuilder;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Endpoint Override URL with FlowFile-attribute Expression Language. Stock AWS processors only evaluate this
 * property against the environment, and bake the result into an S3 client created in {@code onScheduled} — so
 * {@code ${source_endpoint_override}} is empty at start-up and the client talks to AWS instead of HPC.
 */
final class S3FlowFileEndpointSupport {

    static final PropertyDescriptor ENDPOINT_OVERRIDE = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AbstractAwsProcessor.ENDPOINT_OVERRIDE)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private S3FlowFileEndpointSupport() {
    }

    static List<PropertyDescriptor> withFlowFileEndpoint(final List<PropertyDescriptor> descriptors) {
        final List<PropertyDescriptor> replaced = new ArrayList<>(descriptors.size());
        for (final PropertyDescriptor descriptor : descriptors) {
            if (descriptor.getName().equals(ENDPOINT_OVERRIDE.getName())) {
                replaced.add(ENDPOINT_OVERRIDE);
            } else {
                replaced.add(descriptor);
            }
        }
        return replaced;
    }

    static String resolve(final ProcessContext context, final Map<String, String> attributes) {
        final Map<String, String> elAttributes = attributes != null ? attributes : Map.of();
        final String value = context.getProperty(ENDPOINT_OVERRIDE)
                .evaluateAttributeExpressions(elAttributes)
                .getValue();
        return value == null ? "" : value.trim();
    }

    @SuppressWarnings("rawtypes")
    static void apply(final ProcessContext context, final SdkClientBuilder clientBuilder,
                      final Map<String, String> attributes, final ComponentLog log) {
        final String endpointOverride = resolve(context, attributes);
        if (!endpointOverride.isEmpty()) {
            log.info("Overriding endpoint with {}", endpointOverride);
            clientBuilder.endpointOverride(URI.create(endpointOverride));
        }
    }
}
