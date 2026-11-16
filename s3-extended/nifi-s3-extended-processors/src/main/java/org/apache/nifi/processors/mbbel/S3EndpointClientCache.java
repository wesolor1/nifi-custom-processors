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

import org.apache.nifi.processor.ProcessContext;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static org.apache.nifi.processors.aws.region.RegionUtil.getRegion;

/**
 * S3 clients keyed by region and Endpoint Override URL.
 * <p>
 * {@code AbstractAwsProcessor} caches one client per region and applies the endpoint only when that client is built.
 * A second FlowFile with a different {@code target_endpoint_override} would otherwise keep talking to the first
 * endpoint until the processor is stopped.
 */
final class S3EndpointClientCache {

    private final ConcurrentHashMap<String, S3Client> clients = new ConcurrentHashMap<>();

    S3Client get(final ProcessContext context, final Map<String, String> attributes, final Supplier<S3Client> factory) {
        final String key = cacheKey(context, attributes);
        return clients.computeIfAbsent(key, ignored -> factory.get());
    }

    int size() {
        return clients.size();
    }

    void close() {
        final List<S3Client> open = new ArrayList<>(clients.values());
        clients.clear();
        for (final S3Client client : open) {
            try {
                client.close();
            } catch (final RuntimeException ignored) {
                // A client that fails to close must not keep the processor from stopping.
            }
        }
    }

    static String cacheKey(final ProcessContext context, final Map<String, String> attributes) {
        final Map<String, String> elAttributes = attributes == null ? Map.of() : attributes;
        final Region region = getRegion(context, elAttributes);
        final String regionId = region == null ? "" : region.id();
        return regionId + '\u0000' + S3FlowFileEndpointSupport.resolve(context, elAttributes);
    }
}
