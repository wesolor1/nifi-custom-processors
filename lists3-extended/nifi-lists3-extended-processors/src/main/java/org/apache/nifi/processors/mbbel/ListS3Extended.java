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
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.s3.AbstractS3Processor;
import org.apache.nifi.processors.aws.s3.ListS3;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.scheduling.SchedulingStrategy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@TriggerSerially
@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({"Amazon", "S3", "AWS", "list"})
@SeeAlso({ListS3.class})
@CapabilityDescription("Retrieves a listing of objects from an S3 bucket. For each object that is listed, creates a FlowFile that represents "
        + "the object so that it can be fetched in conjunction with FetchS3Object. "
        + "This is a drop-in variant of ListS3 that additionally accepts incoming FlowFiles as trigger signals so the listing can be wired "
        + "downstream of another processor. When triggered by a FlowFile, that FlowFile's attributes are used to evaluate Expression Language on "
        + "the Bucket and Prefix properties and are copied onto every emitted listing FlowFile (listing attributes take precedence when names "
        + "overlap). The trigger FlowFile is removed so it is not emitted again with the listing results. When no incoming connection is present, "
        + "the processor behaves like the standard ListS3 and lists on its schedule. Note: unlike the standard ListS3, this variant is not annotated "
        + "@PrimaryNodeOnly or @TriggerWhenEmpty so that it can be driven by an upstream connection; when running on a cluster without an incoming "
        + "connection, set the Execution Node to 'Primary Node' to avoid duplicate listings.")
@WritesAttributes({
        @WritesAttribute(attribute = "s3.bucket", description = "The name of the S3 bucket"),
        @WritesAttribute(attribute = "s3.region", description = "The region of the S3 bucket"),
        @WritesAttribute(attribute = "filename", description = "The name of the file"),
        @WritesAttribute(attribute = "s3.etag", description = "The ETag that can be used to see if the file has changed"),
        @WritesAttribute(attribute = "s3.isLatest", description = "A boolean indicating if this is the latest version of the object"),
        @WritesAttribute(attribute = "s3.lastModified", description = "The last modified time in milliseconds since epoch in UTC time"),
        @WritesAttribute(attribute = "s3.length", description = "The size of the object in bytes"),
        @WritesAttribute(attribute = "s3.storeClass", description = "The storage class of the object"),
        @WritesAttribute(attribute = "s3.version", description = "The version of the object, if applicable"),
        @WritesAttribute(attribute = "s3.tag.___", description = "If 'Write Object Tags' is set to 'True', the tags associated to the S3 object that is being listed "
                + "will be written as part of the FlowFile attributes"),
        @WritesAttribute(attribute = "s3.user.metadata.___", description = "If 'Write User Metadata' is set to 'True', the user defined metadata associated to the S3 object that is being listed "
                + "will be written as part of the FlowFile attributes")})
@Stateful(scopes = Scope.CLUSTER, description = "After performing a listing of keys, the timestamp of the newest key is stored, "
        + "along with the keys that share that same timestamp. This allows the Processor to list only keys that have been added or modified after "
        + "this date the next time that the Processor is run. State is stored across the cluster so that this Processor can be run on Primary Node only and if a new Primary "
        + "Node is selected, the new node can pick up where the previous node left off, without duplicating the data.")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class ListS3Extended extends ListS3 {

    /**
     * Bucket descriptor overridden so the bucket name can be driven by incoming trigger FlowFile attributes
     * (e.g. {@code ${s3.bucket}}). Keeps the same name ("Bucket") and all other settings of the stock descriptor;
     * only the Expression Language scope is widened to FLOWFILE_ATTRIBUTES.
     */
    public static final PropertyDescriptor BUCKET = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AbstractS3Processor.BUCKET_WITHOUT_DEFAULT_VALUE)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    /**
     * Prefix descriptor overridden so the prefix can be driven by incoming trigger FlowFile attributes. The stock
     * descriptor only supports ENVIRONMENT-scope Expression Language.
     */
    public static final PropertyDescriptor PREFIX = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(ListS3.PREFIX)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private static final Set<String> FLOWFILE_EL_PROPERTY_NAMES = Set.of(BUCKET.getName(), PREFIX.getName());

    /**
     * Snapshot of the last trigger FlowFile's attributes, used both for Expression Language during the listing and
     * for copying onto every emitted FlowFile. Always cleared in a {@code finally} block.
     */
    private final ThreadLocal<Map<String, String>> triggerFlowAttributes = new ThreadLocal<>();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> baseDescriptors = super.getSupportedPropertyDescriptors();
        final List<PropertyDescriptor> descriptors = new ArrayList<>(baseDescriptors.size());
        for (final PropertyDescriptor descriptor : baseDescriptors) {
            if (descriptor.getName().equals(BUCKET.getName())) {
                descriptors.add(BUCKET);
            } else if (descriptor.getName().equals(PREFIX.getName())) {
                descriptors.add(PREFIX);
            } else {
                descriptors.add(descriptor);
            }
        }
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        Map<String, String> triggerAttributes = null;

        if (context.hasIncomingConnection()) {
            final FlowFile incoming = session.get();

            // No work to do yet: an incoming connection is wired but empty. Yield rather than perform a listing,
            // so the processor stays strictly trigger-driven when wired downstream of another processor.
            if (incoming == null) {
                if (context.hasNonLoopConnection()) {
                    context.yield();
                    return;
                }
            } else {
                triggerAttributes = new HashMap<>(incoming.getAttributes());
                session.remove(incoming);
            }
        }

        triggerFlowAttributes.set(triggerAttributes);
        try {
            final ProcessContext listingContext = triggerAttributes == null
                    ? context
                    : new FlowFileAwareProcessContext(context, triggerFlowAttributes);
            final ProcessSession listingSession = withTriggerAttributes(session, triggerAttributes);
            performListing(listingContext, listingSession);
        } finally {
            triggerFlowAttributes.remove();
        }
    }

    /**
     * Seam that performs the actual S3 listing. By default this delegates to the stock {@link ListS3#onTrigger}
     * logic. Exposed as a protected method primarily so the trigger-handling and attribute-merging behavior of this
     * subclass can be unit tested without requiring a live S3 endpoint.
     */
    protected void performListing(final ProcessContext context, final ProcessSession session) {
        super.onTrigger(context, session);
    }

    /**
     * Wrap the session so that every FlowFile created during the listing starts out with the trigger FlowFile's
     * attributes. Listing-specific attributes added afterwards via {@code putAllAttributes} take precedence when names
     * overlap, matching the behavior of the other extended List processors.
     */
    private static ProcessSession withTriggerAttributes(final ProcessSession session,
                                                        final Map<String, String> triggerAttributes) {
        if (triggerAttributes == null || triggerAttributes.isEmpty()) {
            return session;
        }

        final InvocationHandler handler = (proxy, method, args) -> {
            if ("putAllAttributes".equals(method.getName()) && args != null && args.length == 2 && args[1] instanceof Map) {
                @SuppressWarnings("unchecked")
                final Map<String, String> listingAttributes = (Map<String, String>) args[1];
                final Map<String, String> merged = new HashMap<>(triggerAttributes);
                merged.putAll(listingAttributes);
                args[1] = merged;
            }
            return method.invoke(session, args);
        };

        return (ProcessSession) Proxy.newProxyInstance(
                ProcessSession.class.getClassLoader(),
                new Class<?>[] {ProcessSession.class},
                handler);
    }

    /**
     * Delegating {@link ProcessContext} that resolves Expression Language on the overridden Bucket and Prefix
     * properties against the trigger FlowFile's attributes. All other calls pass straight through to the real context.
     */
    private static final class FlowFileAwareProcessContext implements ProcessContext {
        private final ProcessContext delegate;
        private final ThreadLocal<Map<String, String>> attributeSnapshot;

        FlowFileAwareProcessContext(final ProcessContext delegate, final ThreadLocal<Map<String, String>> attributeSnapshot) {
            this.delegate = delegate;
            this.attributeSnapshot = attributeSnapshot;
        }

        @Override
        public PropertyValue getProperty(final PropertyDescriptor descriptor) {
            final PropertyDescriptor resolvedDescriptor;
            if (BUCKET.getName().equals(descriptor.getName())) {
                resolvedDescriptor = BUCKET;
            } else if (PREFIX.getName().equals(descriptor.getName())) {
                resolvedDescriptor = PREFIX;
            } else {
                resolvedDescriptor = descriptor;
            }

            final PropertyValue delegateValue = delegate.getProperty(resolvedDescriptor);
            final Map<String, String> attributes = attributeSnapshot.get();
            if (attributes != null && !attributes.isEmpty()
                    && FLOWFILE_EL_PROPERTY_NAMES.contains(resolvedDescriptor.getName())
                    && resolvedDescriptor.getExpressionLanguageScope() == ExpressionLanguageScope.FLOWFILE_ATTRIBUTES) {
                return delegateValue.evaluateAttributeExpressions(attributes);
            }
            return delegateValue;
        }

        @Override
        public PropertyValue getProperty(final String propertyName) {
            return delegate.getProperty(propertyName);
        }

        @Override
        public Map<String, String> getAllProperties() {
            return delegate.getAllProperties();
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
