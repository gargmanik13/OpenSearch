/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.remote;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ChecksumValidatorTests extends OpenSearchTestCase {

    private static final String TEST_CODEC = "test-codec";
    private static final int VERSION = 1;

    private ChecksumValidator validator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        validator = new ChecksumValidator();
    }

    public void testValidateAndExtractWithValidData() throws IOException {
        byte[] content = "test content data".getBytes(StandardCharsets.UTF_8);
        byte[] blobData = createChecksumBlob(TEST_CODEC, VERSION, content);

        byte[] extracted = validator.validateAndExtract(blobData, TEST_CODEC, VERSION, VERSION);

        assertArrayEquals(content, extracted);
    }

    public void testValidateAndExtractWithEmptyContent() throws IOException {
        byte[] content = new byte[0];
        byte[] blobData = createChecksumBlob(TEST_CODEC, VERSION, content);

        byte[] extracted = validator.validateAndExtract(blobData, TEST_CODEC, VERSION, VERSION);

        assertArrayEquals(content, extracted);
    }

    public void testValidateAndExtractWithLargeContent() throws IOException {
        byte[] content = randomByteArrayOfLength(10000);
        byte[] blobData = createChecksumBlob(TEST_CODEC, VERSION, content);

        byte[] extracted = validator.validateAndExtract(blobData, TEST_CODEC, VERSION, VERSION);

        assertArrayEquals(content, extracted);
    }

    public void testValidateAndExtractWithNullDataThrowsException() {
        CorruptMetadataException ex = expectThrows(
            CorruptMetadataException.class,
            () -> validator.validateAndExtract(null, TEST_CODEC, VERSION, VERSION)
        );
        assertTrue(ex.getMessage().contains("Empty or null data"));
    }

    public void testValidateAndExtractWithEmptyDataThrowsException() {
        CorruptMetadataException ex = expectThrows(
            CorruptMetadataException.class,
            () -> validator.validateAndExtract(new byte[0], TEST_CODEC, VERSION, VERSION)
        );
        assertTrue(ex.getMessage().contains("Empty or null data"));
    }

    public void testValidateAndExtractWithCorruptedDataThrowsException() {
        byte[] content = "test content".getBytes(StandardCharsets.UTF_8);
        byte[] blobData = createChecksumBlob(TEST_CODEC, VERSION, content);

        // Corrupt the data by modifying a byte in the middle
        blobData[blobData.length / 2] ^= 0xFF;

        expectThrows(CorruptMetadataException.class, () -> validator.validateAndExtract(blobData, TEST_CODEC, VERSION, VERSION));
    }

    public void testValidateAndExtractWithWrongCodecThrowsException() throws IOException {
        byte[] content = "test content".getBytes(StandardCharsets.UTF_8);
        byte[] blobData = createChecksumBlob(TEST_CODEC, VERSION, content);

        expectThrows(CorruptMetadataException.class, () -> validator.validateAndExtract(blobData, "wrong-codec", VERSION, VERSION));
    }

    public void testValidateAndExtractWithVersionTooOldThrowsException() throws IOException {
        byte[] content = "test content".getBytes(StandardCharsets.UTF_8);
        byte[] blobData = createChecksumBlob(TEST_CODEC, VERSION, content);

        // Expect version 2 minimum, but blob has version 1
        expectThrows(CorruptMetadataException.class, () -> validator.validateAndExtract(blobData, TEST_CODEC, 2, 2));
    }

    public void testValidateAndExtractWithVersionTooNewThrowsException() throws IOException {
        byte[] content = "test content".getBytes(StandardCharsets.UTF_8);
        byte[] blobData = createChecksumBlob(TEST_CODEC, 2, content);

        // Expect version 1 maximum, but blob has version 2
        expectThrows(CorruptMetadataException.class, () -> validator.validateAndExtract(blobData, TEST_CODEC, 1, 1));
    }

    public void testGetContentBoundsWithValidData() throws IOException {
        byte[] content = "test content data".getBytes(StandardCharsets.UTF_8);
        byte[] blobData = createChecksumBlob(TEST_CODEC, VERSION, content);

        long[] bounds = validator.getContentBounds(blobData, TEST_CODEC, VERSION, VERSION);

        assertEquals(2, bounds.length);
        long contentStart = bounds[0];
        long contentEnd = bounds[1];
        assertEquals(content.length, contentEnd - contentStart);
    }

    public void testGetContentBoundsWithNullDataThrowsException() {
        CorruptMetadataException ex = expectThrows(
            CorruptMetadataException.class,
            () -> validator.getContentBounds(null, TEST_CODEC, VERSION, VERSION)
        );
        assertTrue(ex.getMessage().contains("Empty or null data"));
    }

    public void testRoundTripWithRandomData() throws IOException {
        for (int i = 0; i < 50; i++) {
            byte[] content = randomByteArrayOfLength(randomIntBetween(1, 5000));
            byte[] blobData = createChecksumBlob(TEST_CODEC, VERSION, content);

            byte[] extracted = validator.validateAndExtract(blobData, TEST_CODEC, VERSION, VERSION);

            assertArrayEquals("Round-trip failed for iteration " + i, content, extracted);
        }
    }

    /**
     * Creates a blob with Lucene checksum format: [Header][Content][Footer]
     */
    private byte[] createChecksumBlob(String codec, int version, byte[] content) {
        try {
            ByteBuffersDataOutput dataOutput = new ByteBuffersDataOutput();
            IndexOutput indexOutput = new ByteBuffersIndexOutput(dataOutput, "test", "test");

            CodecUtil.writeHeader(indexOutput, codec, version);
            indexOutput.writeBytes(content, content.length);
            CodecUtil.writeFooter(indexOutput);
            indexOutput.close();

            return dataOutput.toArrayCopy();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create checksum blob", e);
        }
    }
}
