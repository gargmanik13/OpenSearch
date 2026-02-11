/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.index.model;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * A model class for Context metadata that can be used in metadata lib without
 * depending on the full Context class from server module.
 */
@ExperimentalApi
public final class ContextModel implements Writeable, ToXContentFragment {

    private static final String NAME_FIELD = "name";
    private static final String VERSION_FIELD = "version";
    private static final String PARAMS_FIELD = "params";

    private final String name;
    private final String version;
    private final Map<String, Object> params;

    /**
     * Creates a ContextModel with the given fields.
     *
     * @param name the context name
     * @param version the context version
     * @param params the context parameters
     */
    public ContextModel(String name, String version, Map<String, Object> params) {
        this.name = name;
        this.version = version;
        this.params = params;
    }

    /**
     * Creates a ContextModel by reading from StreamInput.
     * Wire format is compatible with Context(StreamInput).
     *
     * @param in the stream to read from
     * @throws IOException if an I/O error occurs
     */
    public ContextModel(StreamInput in) throws IOException {
        this.name = in.readString();
        this.version = in.readOptionalString();
        this.params = in.readMap();
    }

    /**
     * Writes to StreamOutput.
     * Wire format is compatible with Context.writeTo.
     *
     * @param out the stream to write to
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeOptionalString(version);
        out.writeMap(params);
    }

    /**
     * Returns the context name.
     *
     * @return the name
     */
    public String name() {
        return name;
    }

    /**
     * Returns the context version.
     *
     * @return the version
     */
    public String version() {
        return version;
    }

    /**
     * Returns the context parameters.
     *
     * @return the params map
     */
    public Map<String, Object> params() {
        return params;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ContextModel that = (ContextModel) o;
        return Objects.equals(name, that.name) && Objects.equals(version, that.version) && Objects.equals(params, that.params);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, version, params);
    }

    /**
     * Parses a ContextModel from XContent.
     * Expects the parser to be positioned at the start of a context object.
     *
     * @param parser the XContent parser
     * @return the parsed ContextModel
     * @throws IOException if parsing fails
     */
    public static ContextModel fromXContent(XContentParser parser) throws IOException {
        String name = null;
        String version = null;
        Map<String, Object> params = null;

        XContentParser.Token token;
        String currentFieldName = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (NAME_FIELD.equals(currentFieldName)) {
                    name = parser.text();
                } else if (VERSION_FIELD.equals(currentFieldName)) {
                    version = parser.text();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (PARAMS_FIELD.equals(currentFieldName)) {
                    params = parser.map();
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.VALUE_NULL) {
                // Handle null values - just skip them
            }
        }

        return new ContextModel(name, version, params);
    }

    /**
     * Writes this ContextModel to XContent.
     * Outputs context fields within the current object context.
     *
     * @param builder the XContent builder
     * @param params the ToXContent params
     * @return the XContent builder
     * @throws IOException if writing fails
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(NAME_FIELD, name);
        if (version != null) {
            builder.field(VERSION_FIELD, version);
        }
        if (this.params != null) {
            builder.field(PARAMS_FIELD, this.params);
        }
        return builder;
    }
}
