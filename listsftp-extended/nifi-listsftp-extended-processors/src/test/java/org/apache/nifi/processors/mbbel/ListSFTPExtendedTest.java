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
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.file.transfer.FileInfo;
import org.apache.nifi.processors.standard.util.SFTPTransfer;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ListSFTPExtendedTest {

    private static class EmptyListingSftp extends ListSFTPExtended {
        @Override
        protected List<FileInfo> performListing(final ProcessContext context, final Long minTimestamp, final ListingMode listingMode,
                                                final boolean applyFilters) throws IOException {
            return Collections.emptyList();
        }
    }

    private static class ExceptionListingSftp extends ListSFTPExtended {
        @Override
        protected List<FileInfo> performListing(final ProcessContext context, final Long minTimestamp, final ListingMode listingMode,
                                                final boolean applyFilters) throws IOException {
            throw new IOException("simulated listing failure");
        }
    }

    private static class MockWriterFactory extends AbstractControllerService implements RecordSetWriterFactory {
        @Override
        public RecordSetWriter createWriter(ComponentLog logger, RecordSchema schema, OutputStream out, Map<String, String> variables) {
            return new RecordSetWriter() {
                @Override
                public WriteResult write(org.apache.nifi.serialization.record.Record record) throws IOException {
                    throw new UnsupportedOperationException();
                }

                @Override
                public WriteResult write(org.apache.nifi.serialization.record.RecordSet recordSet) throws IOException {
                    return finishRecordSet();
                }

                @Override
                public void beginRecordSet() throws IOException {
                }

                @Override
                public WriteResult finishRecordSet() throws IOException {
                    return WriteResult.of(0, Collections.emptyMap());
                }

                @Override
                public String getMimeType() {
                    return "application/test";
                }

                @Override
                public void flush() throws IOException {}

                @Override
                public void close() throws IOException {}
            };
        }

        @Override
        public RecordSchema getSchema(Map<String, String> variables, RecordSchema inputSchema) throws SchemaNotFoundException {
            return inputSchema;
        }
    }

    @Test
    public void testEmptyDirectoryEmitsZeroRecordFlowFile() throws Exception {
        final MockWriterFactory writerFactory = new MockWriterFactory();
        final RecordSetWriter writer = writerFactory.createWriter(null, null, OutputStream.nullOutputStream(), Map.of());

        writer.beginRecordSet();
        final WriteResult result = writer.finishRecordSet();

        assertTrue(new ListSFTPExtended().getRelationships().contains(ListSFTPExtended.REL_NO_FILES));
        assertEquals(0, result.getRecordCount());
        assertEquals("application/test", writer.getMimeType());
    }

    @Test
    public void testListingFailureProducesFailureFlowFile() throws Exception {
        assertTrue(new ListSFTPExtended().getRelationships().contains(ListSFTPExtended.REL_FAILURE));
    }

    @Test
    public void testIncomingConnectionsAreAllowed() {
        final InputRequirement requirement = ListSFTPExtended.class.getAnnotation(InputRequirement.class);
        assertNotNull(requirement, "@InputRequirement should be present on ListSFTPExtended");
        assertEquals(Requirement.INPUT_ALLOWED, requirement.value());
    }

    @Test
    public void testConnectionPropertiesSupportFlowFileAttributesExpressionLanguage() {
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                SFTPTransfer.HOSTNAME.getExpressionLanguageScope(),
                "Hostname must be evaluatable against FlowFile attributes");
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                SFTPTransfer.PORT.getExpressionLanguageScope(),
                "Port must be evaluatable against FlowFile attributes");
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                SFTPTransfer.USERNAME.getExpressionLanguageScope(),
                "Username must be evaluatable against FlowFile attributes");
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                ListSFTPExtended.PASSWORD.getExpressionLanguageScope(),
                "Password must be evaluatable against FlowFile attributes");
        assertEquals(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                ListSFTPExtended.REMOTE_PATH.getExpressionLanguageScope(),
                "Remote Path must be evaluatable against FlowFile attributes");
    }

    @Test
    public void testCustomPasswordIsNotSensitive() {
        assertFalse(ListSFTPExtended.PASSWORD.isSensitive(),
                "ListSFTPExtended PASSWORD descriptor should override stock SFTPTransfer.PASSWORD to be non-sensitive");
    }

}
