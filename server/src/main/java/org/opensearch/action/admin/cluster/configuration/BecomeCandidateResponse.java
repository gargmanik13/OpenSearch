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

/**
 * Response for become candidate operation
 */
public class BecomeCandidateResponse extends ActionResponse implements ToXContentObject {

    private final String nodeId;
    private final String nodeIp;
    private final String currentMasterId;
    private final String currentMasterIp;
    private final boolean masterWasHealthy;

    public BecomeCandidateResponse(String nodeId, String nodeIp, String currentMasterId,
                                   String currentMasterIp, boolean masterWasHealthy) {
        this.nodeId = nodeId != null ? nodeId : "";
        this.nodeIp = nodeIp != null ? nodeIp : "";
        this.currentMasterId = currentMasterId;
        this.currentMasterIp = currentMasterIp;
        this.masterWasHealthy = masterWasHealthy;
    }

    public BecomeCandidateResponse(StreamInput in) throws IOException {
        super(in);
        nodeId = in.readString();
        nodeIp = in.readString();
        currentMasterId = in.readOptionalString();
        currentMasterIp = in.readOptionalString();
        masterWasHealthy = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeId);
        out.writeString(nodeIp);
        out.writeOptionalString(currentMasterId);
        out.writeOptionalString(currentMasterIp);
        out.writeBoolean(masterWasHealthy);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field("node_id", nodeId);
        builder.field("node_ip", nodeIp);
        if (currentMasterId != null) {
            builder.field("current_master_id", currentMasterId);
        }
        if (currentMasterIp != null) {
            builder.field("current_master_ip", currentMasterIp);
        }
        builder.field("master_was_healthy", masterWasHealthy);
        builder.endObject();
        return builder;
    }

    // Getters
    public String getNodeId() { return nodeId; }
    public String getNodeIp() { return nodeIp; }
    public String getCurrentMasterId() { return currentMasterId; }
    public String getCurrentMasterIp() { return currentMasterIp; }
    public boolean isMasterWasHealthy() { return masterWasHealthy; }
}
