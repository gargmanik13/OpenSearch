/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.configuration;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * A response to {@link RemoveUnhealthyNodesRequest} indicating that unhealthy nodes have been
 * removed from the cluster.
 *
 * @opensearch.internal
 */
public class RemoveUnhealthyNodesResponse extends ActionResponse implements ToXContentObject {

    private final List<String> removedNodeIds;
    private final int totalRemovedCount;

    public RemoveUnhealthyNodesResponse() {
        this.removedNodeIds = List.of();
        this.totalRemovedCount = 0;
    }

    public RemoveUnhealthyNodesResponse(List<String> removedNodeIds) {
        this.removedNodeIds = removedNodeIds != null ? removedNodeIds : List.of();
        this.totalRemovedCount = this.removedNodeIds.size();
    }

    public RemoveUnhealthyNodesResponse(StreamInput in) throws IOException {
        super(in);
        this.removedNodeIds = in.readStringList();
        this.totalRemovedCount = in.readVInt();
    }

    public List<String> getRemovedNodeIds() {
        return removedNodeIds;
    }

    public int getTotalRemovedCount() {
        return totalRemovedCount;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(removedNodeIds);
        out.writeVInt(totalRemovedCount);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field("removed_nodes", removedNodeIds);
        builder.field("total_removed_count", totalRemovedCount);
        builder.endObject();
        return builder;
    }
}
