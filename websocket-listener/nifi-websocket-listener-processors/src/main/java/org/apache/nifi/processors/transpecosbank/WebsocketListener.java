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
package org.apache.nifi.processors.transpecosbank;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.util.StandardValidators;
import org.eclipse.jetty.websocket.api.Callback;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.*;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;


@Tags({"TestWebsocketListener", "Websocket", "Connection"})
@CapabilityDescription("Listener of Websocket API.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "websocket.remote.address", description = "")})
@WebSocket
public class WebsocketListener extends AbstractProcessor {

    private static final long MAX_MESSAGE_SIZE = 1048576 ;

    protected volatile ProcessSession processSession;
    private WebSocketClient client;
    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
    private Session session;

    public static final PropertyDescriptor HOST = new PropertyDescriptor
            .Builder().name("Host")
            .displayName("WS Host")
            .description("Example Property")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    public static final Relationship CONNECTED = new Relationship.Builder()
            .name("connected")
            .description("The WebSocket session is established")
            .build();

    public static final Relationship NOT_CONNECTED = new Relationship.Builder()
            .name("not connected")
            .description("The WebSocket session is not established")
            .build();

    public static final Relationship REL_MESSAGE = new Relationship.Builder()
            .name("text message")
            .description("The WebSocket text message output")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFile holding connection configuration attributes (like URL or HTTP headers) in case of connection failure")
            .autoTerminateDefault(true)
            .build();



    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(HOST);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(CONNECTED);
        relationships.add(NOT_CONNECTED);
        relationships.add(FAILURE);
        relationships.add(REL_MESSAGE);
        relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
    }


    @Override
    public void onTrigger(final ProcessContext processContext, final ProcessSession processSession) {
        Objects.requireNonNull(processSession, "Process session cannot be null");

        this.processSession = processSession;

        FlowFile flowFile = processSession.get();
        if (flowFile == null) {
            return;
        }

        try {
            String host = processContext.getProperty(HOST).getValue();
            getComponentLogger().info("Connection host: ", host);
            establishWebSocketConnection(host);
            processSession.transfer(flowFile, CONNECTED);

        } catch (Exception e) {

            getComponentLogger().error("Websocket connection error", e);
            flowFile = processSession.putAttribute(flowFile, "error.message", e.getMessage());
            processSession.transfer(flowFile, NOT_CONNECTED);
        }
    }


    @OnStopped
    public void onStopped() {
        getComponentLogger().info("WebSocket connection - stopped");

        // Close the WebSocket session when the processor is stopped
        if (this.client != null) {
            try {
                this.client.stop();
            } catch (Exception e) {
                getComponentLogger().error("Failed to stop WebSocket client", e);
            }
        }
    }


    @OnWebSocketMessage
    public void onMessage(String message, Session session) {
        this.session = session;
        final ProcessSession processSession = getProcessSession();

        if (processSession == null) {
            getComponentLogger().error("Process session is null. Cannot process message: {}", message);
            return;
        }
        // Create a new FlowFile
        FlowFile flowFile = processSession.create();
        // Get the remote address of the WebSocket server
        String ipAddress = session.getRemoteSocketAddress().toString();

        // Add the IP address as an attribute to the FlowFile
        flowFile = processSession.putAttribute(flowFile, "websocket.remote.address", ipAddress);

        try {

            // Write the received WebSocket message to the FlowFile
            flowFile = writeMessageToFlowFile(processSession, flowFile, message);
            sendAcknowledgement(message, session);
            // Transfer the FlowFile to the 'CONNECTED' relationship
            processSession.transfer(flowFile, REL_MESSAGE);

            // Commit the session to indicate that the FlowFile processing is complete
            processSession.commitAsync();
        } catch (Exception e) {
            getComponentLogger().error("Error processing WebSocket message", e);

            // Transfer the FlowFile to the 'FAILURE' relationship
            processSession.transfer(flowFile, FAILURE);
        }
    }

    @OnWebSocketOpen
    public void onOpen(Session session) {
        this.session = session;
        // The WebSocket endpoint has been opened.
        getComponentLogger().info("WebSocket connection opened");

    }


    @OnWebSocketError
    public void onWebSocketError(Throwable cause) {
        // The WebSocket endpoint failed.
        // You may log the error.
        getComponentLogger().error("WebSocket error", cause);
        // You may dispose resources.
    }

    @OnWebSocketClose
    public void onWebSocketClose(int statusCode, String reason) {
        // The WebSocket endpoint has been closed.
        getComponentLogger().info("WebSocket connection closed with statusCode: {} and reason: {}", statusCode, reason);
        // You may dispose resources.
    }


    synchronized ProcessSession getProcessSession() {
        return this.processSession;
    }

    FlowFile writeMessageToFlowFile(ProcessSession processSession, FlowFile flowFile, String message) throws IOException {
        Objects.requireNonNull(processSession, "Process session cannot be null");
        Objects.requireNonNull(flowFile, "Flow file cannot be null");
        Objects.requireNonNull(message, "Message cannot be null");
        return processSession.write(flowFile, out -> out.write(message.getBytes(StandardCharsets.UTF_8)));
    }

    private void establishWebSocketConnection(String host) throws Exception {

        this.client = new WebSocketClient();
        this.client.setMaxTextMessageSize(MAX_MESSAGE_SIZE);
        this.client.setMaxFrameSize(MAX_MESSAGE_SIZE);
        this.client.start();


        ClientUpgradeRequest request = new ClientUpgradeRequest();
        this.client.connect(this, URI.create(host), request);

        getComponentLogger().info("Websocket connection established");
    }

    private void sendAcknowledgement(String message, Session session) {
        try {

            ObjectMapper mapper = new ObjectMapper();
            JsonNode jsonObject = mapper.readTree(message);
            String id = jsonObject.get("id").asText();

            if (!jsonObject.get("event").asText().startsWith("/")) {
                Map<String, String> ackMessageMap = new HashMap<>();
                ackMessageMap.put("msg", id);
                ackMessageMap.put("event", "ack");
                String ackMessage = mapper.writeValueAsString(ackMessageMap);
                session.sendText(ackMessage, Callback.from(session::demand, Throwable::printStackTrace));
            }
        } catch (IOException e) {
            getComponentLogger().error("Error sending acknowledgement", e);
        }
    }




    public ComponentLog getComponentLogger() {
        return getLogger();
    }
}
