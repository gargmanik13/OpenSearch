/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.remote;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

/**
 * Validates checksums for remote metadata blobs using Lucene's CodecUtil.
 * <p>
 * This class provides checksum validation for metadata stored in remote storage
 * using the same format as ChecksumBlobStoreFormat in the server module.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ChecksumValidator {

    private static final String RESOURCE_DESC_PREFIX = "ChecksumValidator.validateAndExtract";

    /**
     * Creates a new ChecksumValidator instance.
     */
    public ChecksumValidator() {
        // Default constructor
    }

    /**
     * Validates the checksum of the given data and extracts the content bytes.
     * <p>
     * The data is expected to be in Lucene's checksum format:
     * [Header: codec name + version][Content][Footer: checksum]
     *
     * @param data       the raw blob data with Lucene header/footer
     * @param codecName  the expected codec name
     * @param minVersion the minimum supported version
     * @param maxVersion the maximum supported version
     * @return the content bytes without header/footer
     * @throws CorruptMetadataException if checksum validation fails
     * @throws IOException              if an I/O error occurs
     */
    public byte[] validateAndExtract(byte[] data, String codecName, int minVersion, int maxVersion) throws IOException {
        if (data == null || data.length == 0) {
            throw new CorruptMetadataException("Empty or null data provided for checksum validation");
        }

        final String resourceDesc = RESOURCE_DESC_PREFIX + "(codec=\"" + codecName + "\")";

        try {
            IndexInput indexInput = new ByteBuffersIndexInput(
                new ByteBuffersDataInput(Collections.singletonList(ByteBuffer.wrap(data))),
                resourceDesc
            );

            // Validate the entire file checksum first
            CodecUtil.checksumEntireFile(indexInput);

            // Validate header and get version
            CodecUtil.checkHeader(indexInput, codecName, minVersion, maxVersion);

            // Calculate content bounds
            long contentStart = indexInput.getFilePointer();
            long contentEnd = indexInput.length() - CodecUtil.footerLength();
            long contentSize = contentEnd - contentStart;

            if (contentSize < 0) {
                throw new CorruptMetadataException("Invalid content size: " + contentSize);
            }

            // Extract content bytes
            byte[] content = new byte[(int) contentSize];
            indexInput.readBytes(content, 0, (int) contentSize);

            return content;
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            throw new CorruptMetadataException("Metadata checksum validation failed: " + ex.getMessage(), ex);
        }
    }

    /**
     * Returns the content bounds (start and end positions) within the data.
     * <p>
     * This method can be used when you need to know the exact positions
     * without extracting the content.
     *
     * @param data       the raw blob data with Lucene header/footer
     * @param codecName  the expected codec name
     * @param minVersion the minimum supported version
     * @param maxVersion the maximum supported version
     * @return an array of two longs: [contentStart, contentEnd]
     * @throws CorruptMetadataException if checksum validation fails
     * @throws IOException              if an I/O error occurs
     */
    public long[] getContentBounds(byte[] data, String codecName, int minVersion, int maxVersion) throws IOException {
        if (data == null || data.length == 0) {
            throw new CorruptMetadataException("Empty or null data provided for checksum validation");
        }

        final String resourceDesc = RESOURCE_DESC_PREFIX + "(codec=\"" + codecName + "\")";

        try {
            IndexInput indexInput = new ByteBuffersIndexInput(
                new ByteBuffersDataInput(Collections.singletonList(ByteBuffer.wrap(data))),
                resourceDesc
            );

            // Validate the entire file checksum first
            CodecUtil.checksumEntireFile(indexInput);

            // Validate header
            CodecUtil.checkHeader(indexInput, codecName, minVersion, maxVersion);

            long contentStart = indexInput.getFilePointer();
            long contentEnd = indexInput.length() - CodecUtil.footerLength();

            return new long[] { contentStart, contentEnd };
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            throw new CorruptMetadataException("Metadata checksum validation failed: " + ex.getMessage(), ex);
        }
    }
}
