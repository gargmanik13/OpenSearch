/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.index.model;

import org.opensearch.common.util.set.Sets;
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

public class AliasMetadataModelTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        final AliasMetadataModel before = new AliasMetadataModel.Builder("alias").filter(createTestFilter())
            .indexRouting("indexRouting")
            .routing("routing")
            .searchRouting("trim,tw , ltw , lw")
            .writeIndex(randomBoolean() ? null : randomBoolean())
            .isHidden(randomBoolean() ? null : randomBoolean())
            .build();

        assertThat(before.searchRoutingValues(), equalTo(Sets.newHashSet("trim", "tw ", " ltw ", " lw")));

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final StreamOutput out = new OutputStreamStreamOutput(baos);
        before.writeTo(out);
        out.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final StreamInput in = new InputStreamStreamInput(bais);
        final AliasMetadataModel after = new AliasMetadataModel(in);

        assertThat(after, equalTo(before));
        assertEquals(before.hashCode(), after.hashCode());
    }

    public void testEquals() {
        AliasMetadataModel model1 = createTestItem();
        AliasMetadataModel model2 = new AliasMetadataModel(
            model1.alias(),
            model1.filter(),
            model1.indexRouting(),
            model1.searchRouting(),
            model1.writeIndex(),
            model1.isHidden()
        );

        assertNotSame(model1, model2);
        assertEquals(model1, model2);
        assertEquals(model1.hashCode(), model2.hashCode());
    }

    private static AliasMetadataModel createTestItem() {
        AliasMetadataModel.Builder builder = new AliasMetadataModel.Builder(randomAlphaOfLengthBetween(3, 10));
        if (randomBoolean()) {
            builder.routing(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            builder.searchRouting(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            builder.indexRouting(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            builder.filter(createTestFilter());
        }
        builder.writeIndex(randomBoolean());

        if (randomBoolean()) {
            builder.isHidden(randomBoolean());
        }
        return builder.build();
    }

    private static CompressedData createTestFilter() {
        return new CompressedData(randomByteArrayOfLength(randomIntBetween(10, 50)), randomInt());
    }

    private static CompressedData createJsonFilter(String json) throws IOException {
        byte[] bytes = json.getBytes();
        CRC32 crc32 = new CRC32();
        crc32.update(bytes);
        return new CompressedData(bytes, (int) crc32.getValue());
    }

    // XContent Tests

    public void testXContentRoundTrip() throws IOException {
        // Create a model with a valid JSON filter
        CompressedData filter = createJsonFilter("{\"term\":{\"user\":\"kimchy\"}}");
        AliasMetadataModel original = new AliasMetadataModel.Builder("test-alias").filter(filter)
            .indexRouting("index_route")
            .searchRouting("search_route")
            .writeIndex(true)
            .isHidden(false)
            .build();

        // Serialize to XContent
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        original.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        byte[] bytes = BytesReference.bytes(builder).toBytesRef().bytes;

        // Parse back from XContent
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            parser.nextToken(); // START_OBJECT
            parser.nextToken(); // FIELD_NAME (alias name)
            AliasMetadataModel parsed = AliasMetadataModel.fromXContent(parser);

            assertEquals(original.alias(), parsed.alias());
            assertEquals(original.indexRouting(), parsed.indexRouting());
            assertEquals(original.searchRouting(), parsed.searchRouting());
            assertEquals(original.writeIndex(), parsed.writeIndex());
            assertEquals(original.isHidden(), parsed.isHidden());
            assertNotNull(parsed.filter());
        }
    }

    public void testXContentRoundTripWithoutFilter() throws IOException {
        AliasMetadataModel original = new AliasMetadataModel.Builder("test-alias").indexRouting("index_route")
            .searchRouting("search_route")
            .writeIndex(false)
            .build();

        // Serialize to XContent
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        original.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        byte[] bytes = BytesReference.bytes(builder).toBytesRef().bytes;

        // Parse back from XContent
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            parser.nextToken(); // START_OBJECT
            parser.nextToken(); // FIELD_NAME (alias name)
            AliasMetadataModel parsed = AliasMetadataModel.fromXContent(parser);

            assertEquals(original.alias(), parsed.alias());
            assertEquals(original.indexRouting(), parsed.indexRouting());
            assertEquals(original.searchRouting(), parsed.searchRouting());
            assertEquals(original.writeIndex(), parsed.writeIndex());
            assertNull(parsed.filter());
            assertNull(parsed.isHidden());
        }
    }

    public void testXContentRoundTripMinimal() throws IOException {
        AliasMetadataModel original = new AliasMetadataModel.Builder("minimal-alias").build();

        // Serialize to XContent
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        original.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        byte[] bytes = BytesReference.bytes(builder).toBytesRef().bytes;

        // Parse back from XContent
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            parser.nextToken(); // START_OBJECT
            parser.nextToken(); // FIELD_NAME (alias name)
            AliasMetadataModel parsed = AliasMetadataModel.fromXContent(parser);

            assertEquals("minimal-alias", parsed.alias());
            assertNull(parsed.filter());
            assertNull(parsed.indexRouting());
            assertNull(parsed.searchRouting());
            assertNull(parsed.writeIndex());
            assertNull(parsed.isHidden());
        }
    }

    public void testFromXContentWithRouting() throws IOException {
        // Test parsing with the "routing" field which sets both index and search routing
        String json = "{\"my-alias\":{\"routing\":\"shared_route\"}}";
        byte[] bytes = json.getBytes();

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            parser.nextToken(); // START_OBJECT
            parser.nextToken(); // FIELD_NAME (alias name)
            AliasMetadataModel parsed = AliasMetadataModel.fromXContent(parser);

            assertEquals("my-alias", parsed.alias());
            assertEquals("shared_route", parsed.indexRouting());
            assertEquals("shared_route", parsed.searchRouting());
        }
    }
}
