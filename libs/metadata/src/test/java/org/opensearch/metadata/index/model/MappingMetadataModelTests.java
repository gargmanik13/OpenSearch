/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.index.model;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.metadata.compress.CompressedData;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.CRC32;

import static org.hamcrest.Matchers.equalTo;

public class MappingMetadataModelTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        final MappingMetadataModel before = createTestItem();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final StreamOutput out = new OutputStreamStreamOutput(baos);
        before.writeTo(out);
        out.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final StreamInput in = new InputStreamStreamInput(bais);
        final MappingMetadataModel after = new MappingMetadataModel(in);

        assertThat(after, equalTo(before));
        assertThat(after.type(), equalTo(before.type()));
        assertThat(after.source(), equalTo(before.source()));
        assertThat(after.routingRequired(), equalTo(before.routingRequired()));
        assertEquals(before.hashCode(), after.hashCode());
    }

    public void testEquals() {
        MappingMetadataModel model1 = createTestItem();
        MappingMetadataModel model2 = new MappingMetadataModel(model1.type(), model1.source(), model1.routingRequired());

        assertNotSame(model1, model2);
        assertEquals(model1, model2);
        assertEquals(model1.hashCode(), model2.hashCode());
    }

    private static MappingMetadataModel createTestItem() {
        return new MappingMetadataModel(randomAlphaOfLengthBetween(3, 10), createTestSource(), randomBoolean());
    }

    private static CompressedData createTestSource() {
        return new CompressedData(randomByteArrayOfLength(randomIntBetween(10, 100)), randomInt());
    }

    private static CompressedData createJsonSource(String type, String json) throws IOException {
        // Wrap the content with the type name: { "type": { ... content ... } }
        String wrappedJson = "{\"" + type + "\":" + json + "}";
        byte[] bytes = wrappedJson.getBytes();
        CRC32 crc32 = new CRC32();
        crc32.update(bytes);
        return new CompressedData(bytes, (int) crc32.getValue());
    }

    // XContent Tests

    public void testXContentRoundTrip() throws IOException {
        // Create a model with valid JSON mapping source
        String mappingContent = "{\"properties\":{\"field1\":{\"type\":\"text\"},\"field2\":{\"type\":\"keyword\"}}}";
        CompressedData source = createJsonSource("_doc", mappingContent);
        MappingMetadataModel original = new MappingMetadataModel("_doc", source, false);

        // Serialize to XContent
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        original.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        byte[] bytes = BytesReference.bytes(builder).toBytesRef().bytes;

        // Parse back from XContent
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            parser.nextToken(); // START_OBJECT
            parser.nextToken(); // FIELD_NAME (mapping type)
            MappingMetadataModel parsed = MappingMetadataModel.fromXContent(parser);

            assertEquals(original.type(), parsed.type());
            assertEquals(original.routingRequired(), parsed.routingRequired());
            assertNotNull(parsed.source());
        }
    }

    public void testXContentRoundTripWithRoutingRequired() throws IOException {
        // Create a model with routing required
        String mappingContent = "{\"_routing\":{\"required\":true},\"properties\":{\"name\":{\"type\":\"text\"}}}";
        CompressedData source = createJsonSource("_doc", mappingContent);
        MappingMetadataModel original = new MappingMetadataModel("_doc", source, true);

        // Serialize to XContent
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        original.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        byte[] bytes = BytesReference.bytes(builder).toBytesRef().bytes;

        // Parse back from XContent
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            parser.nextToken(); // START_OBJECT
            parser.nextToken(); // FIELD_NAME (mapping type)
            MappingMetadataModel parsed = MappingMetadataModel.fromXContent(parser);

            assertEquals("_doc", parsed.type());
            assertTrue(parsed.routingRequired());
        }
    }

    public void testFromXContentParsesRoutingRequired() throws IOException {
        // Test that fromXContent correctly parses routing required from the mapping content
        String json = "{\"my_type\":{\"_routing\":{\"required\":true},\"properties\":{\"id\":{\"type\":\"keyword\"}}}}";
        byte[] bytes = json.getBytes();

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            parser.nextToken(); // START_OBJECT
            parser.nextToken(); // FIELD_NAME (mapping type)
            MappingMetadataModel parsed = MappingMetadataModel.fromXContent(parser);

            assertEquals("my_type", parsed.type());
            assertTrue(parsed.routingRequired());
        }
    }

    public void testFromXContentRoutingNotRequired() throws IOException {
        // Test that fromXContent correctly parses when routing is not required
        String json = "{\"_doc\":{\"properties\":{\"title\":{\"type\":\"text\"}}}}";
        byte[] bytes = json.getBytes();

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            parser.nextToken(); // START_OBJECT
            parser.nextToken(); // FIELD_NAME (mapping type)
            MappingMetadataModel parsed = MappingMetadataModel.fromXContent(parser);

            assertEquals("_doc", parsed.type());
            assertFalse(parsed.routingRequired());
        }
    }

    public void testXContentMinimalMapping() throws IOException {
        // Test with minimal mapping content
        String mappingContent = "{}";
        CompressedData source = createJsonSource("_doc", mappingContent);
        MappingMetadataModel original = new MappingMetadataModel("_doc", source, false);

        // Serialize to XContent
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        original.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        byte[] bytes = BytesReference.bytes(builder).toBytesRef().bytes;

        // Parse back from XContent
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            parser.nextToken(); // START_OBJECT
            parser.nextToken(); // FIELD_NAME (mapping type)
            MappingMetadataModel parsed = MappingMetadataModel.fromXContent(parser);

            assertEquals("_doc", parsed.type());
            assertFalse(parsed.routingRequired());
        }
    }
}
