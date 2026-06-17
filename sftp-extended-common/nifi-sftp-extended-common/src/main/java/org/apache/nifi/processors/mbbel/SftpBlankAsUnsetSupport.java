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
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.standard.util.SFTPTransfer;
import org.apache.nifi.scheduling.ExecutionNode;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.Set;

/**
 * Treats blank SFTP property values as unset ({@code null}). Used by extended SFTP processors so the stock
 * {@code StandardSshClientProvider} can fall back between password and private-key authentication when Expression
 * Language resolves one credential to an empty string.
 */
public final class SftpBlankAsUnsetSupport {

    public static final Set<String> AUTH_PROPERTY_NAMES = Set.of(
            SFTPTransfer.PASSWORD.getName(),
            SFTPTransfer.PRIVATE_KEY_PATH.getName(),
            SFTPTransfer.PRIVATE_KEY_PASSPHRASE.getName()
    );

    private SftpBlankAsUnsetSupport() {
    }

    public static boolean isAuthProperty(final String propertyName) {
        return propertyName != null && AUTH_PROPERTY_NAMES.contains(propertyName);
    }

    public static ProcessContext blankAsUnsetContext(final ProcessContext delegate) {
        return blankAsUnsetContext(delegate, AUTH_PROPERTY_NAMES);
    }

    public static ProcessContext blankAsUnsetContext(final ProcessContext delegate, final Set<String> propertyNames) {
        return new BlankAsUnsetProcessContext(delegate, propertyNames);
    }

    public static PropertyValue blankAsUnset(final PropertyValue delegate) {
        return (PropertyValue) Proxy.newProxyInstance(
                SftpBlankAsUnsetSupport.class.getClassLoader(),
                new Class<?>[]{PropertyValue.class},
                new BlankAsUnsetPropertyValueHandler(delegate));
    }

    private static final class BlankAsUnsetPropertyValueHandler implements InvocationHandler {
        private final PropertyValue delegate;

        private BlankAsUnsetPropertyValueHandler(final PropertyValue delegate) {
            this.delegate = delegate;
        }

        @Override
        public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
            final String name = method.getName();
            final Object result;
            try {
                result = method.invoke(delegate, args);
            } catch (final InvocationTargetException e) {
                throw e.getCause();
            }

            if ("getValue".equals(name) && (args == null || args.length == 0)) {
                final String value = (String) result;
                return (value == null || value.isBlank()) ? null : value;
            }
            if (name.startsWith("evaluateAttributeExpressions") && result instanceof PropertyValue propertyValue) {
                return blankAsUnset(propertyValue);
            }
            return result;
        }
    }

    private static final class BlankAsUnsetProcessContext implements ProcessContext {
        private final ProcessContext delegate;
        private final Set<String> blankAsUnsetPropertyNames;

        private BlankAsUnsetProcessContext(final ProcessContext delegate, final Set<String> blankAsUnsetPropertyNames) {
            this.delegate = delegate;
            this.blankAsUnsetPropertyNames = blankAsUnsetPropertyNames;
        }

        @Override
        public PropertyValue getProperty(final PropertyDescriptor descriptor) {
            final PropertyValue value = delegate.getProperty(descriptor);
            if (descriptor != null && blankAsUnsetPropertyNames.contains(descriptor.getName())) {
                return blankAsUnset(value);
            }
            return value;
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
        public Map<String, String> getAllProperties() {
            return delegate.getAllProperties();
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
