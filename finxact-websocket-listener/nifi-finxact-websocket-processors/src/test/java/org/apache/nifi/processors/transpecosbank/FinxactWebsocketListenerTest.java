package org.apache.nifi.processors.transpecosbank;


import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;

import org.apache.nifi.processor.ProcessSession;
import org.eclipse.jetty.websocket.api.Session;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import static org.mockito.Mockito.*;

public class FinxactWebsocketListenerTest {

    private FinxactWebsocketListener processor;

    @BeforeEach
    public void setUp() {
        processor = spy(new FinxactWebsocketListener());
        ComponentLog mockLogger = mock(ComponentLog.class);
        doReturn(mockLogger).when(processor).getComponentLogger();
    }


    @Test
    public void testOnMessage() throws IOException, URISyntaxException {
        // Arrange
        String testMessage = "{\"id\":\"a067a663-ffde-4504-8aee-6e76943228dd\",\"event\":\"model.v1.folder.create\",\"client\":{\"user_id\":\"nifi-custom-test1\",\"connection_id\":\"b1266121-7f14-4991-ba7e-d397b5ff272f\"},\"msg\":{\"correlationID\":\"4wOvqf5SzOYiBoaVt1Zhalcp\",\"eventKey\":\"model.v1.folder.create\",\"eventDtm\":\"2024-04-15T14:54:47.474301733Z\",\"source\":\"nGageUI\",\"resourceURI\":\"\",\"version\":0,\"data\":{\"_Id\":\"4wOvqf5WemJEyBY0iNGTgoXr\",\"_cDtm\":\"2024-04-15T14:54:47.474301733Z\",\"_cLogRef\":\"4wOvqf5SzOYiBoaVt1Zhalcp\",\"_flags\":0,\"_schVn\":0,\"_uDtm\":\"2024-04-15T14:54:47.474301733Z\",\"_uLog\":\"4wOvqf5WemJEyBY0iNH3LctP\",\"_vn\":0},\"journal\":{},\"objectId\":\"{\\\"_uLog\\\":\\\"4wOvqf5WemJEyBY0iNH3LctP\\\",\\\"table\\\":\\\"folder\\\"}\"}}";
        Session mockSession = mock(Session.class);
        ProcessSession mockProcessSession = mock(ProcessSession.class);
        FlowFile mockFlowFile = mock(FlowFile.class);

        // Mock the getRemoteSocketAddress method to return an InetSocketAddress
        InetSocketAddress mockAddress = new InetSocketAddress("localhost", 8080);
        when(mockSession.getRemoteSocketAddress()).thenReturn(mockAddress);

        // Mock the getProcessSession method to return the mock ProcessSession
        doReturn(mockProcessSession).when(processor).getProcessSession();

        // Mock the writeMessageToFlowFile method to return the mock FlowFile
        doReturn(mockFlowFile).when(processor).writeMessageToFlowFile(eq(mockProcessSession), eq(mockFlowFile), eq(testMessage));

        // Act
        processor.onMessage(testMessage, mockSession);

        // Assert
        // Verify that the getProcessSession method was called
        verify(processor, times(1)).getProcessSession();
    }
}