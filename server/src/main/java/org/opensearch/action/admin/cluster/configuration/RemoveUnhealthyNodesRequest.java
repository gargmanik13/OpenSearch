/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.configuration;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * A request to remove unhealthy nodes by specifying the IP addresses of healthy nodes.
 * This will remove all nodes that are NOT in the healthy nodes IP list.
 *
 * @opensearch.internal
 */
public class RemoveUnhealthyNodesRequest extends ClusterManagerNodeRequest<RemoveUnhealthyNodesRequest> {

    private final String[] healthyNodeIps;
    private final TimeValue timeout;

    /**
     * Construct a request to remove unhealthy nodes by keeping only nodes with the specified IP addresses.
     * @param healthyNodeIps IP addresses of the nodes that should remain active
     * @param timeout How long to wait for the exclusions to take effect
     */
    public RemoveUnhealthyNodesRequest(String[] healthyNodeIps, TimeValue timeout) {
        if (timeout.compareTo(TimeValue.ZERO) < 0) {
            throw new IllegalArgumentException("timeout [" + timeout + "] must be non-negative");
        }

        if (healthyNodeIps == null || healthyNodeIps.length == 0) {
            throw new IllegalArgumentException("At least one healthy node IP must be specified");
        }

        this.healthyNodeIps = healthyNodeIps;
        this.timeout = timeout;
    }

    public RemoveUnhealthyNodesRequest(StreamInput in) throws IOException {
        super(in);
        healthyNodeIps = in.readStringArray();
        timeout = in.readTimeValue();
    }

    /**
     * Resolves unhealthy nodes that should be removed from the cluster.
     * Compares cluster nodes with healthy IP list and identifies nodes to remove.
     * @param currentState Current cluster state
     * @return Set of unhealthy nodes to be removed
     */
    Set<DiscoveryNode> resolveUnhealthyNodesForRemoval(ClusterState currentState) {
        final DiscoveryNodes allNodes = currentState.nodes();
        Set<String> healthyNodeIpSet = new HashSet<>(Arrays.asList(healthyNodeIps));

        Set<DiscoveryNode> unhealthyNodes = new HashSet<>();

        // Find all nodes that are NOT in the healthy IP list
        for (DiscoveryNode node : allNodes) {
            String nodeIp = extractIpFromNode(node);

            // If node IP is not in the healthy list, it's considered unhealthy and should be removed
            if (nodeIp != null && !healthyNodeIpSet.contains(nodeIp)) {
                unhealthyNodes.add(node);
            }
        }

        return unhealthyNodes;
    }

    /**
     * Extracts IP address from a DiscoveryNode.
     * @param node The discovery node
     * @return IP address as string
     */
    private String extractIpFromNode(DiscoveryNode node) {
        if (node.getAddress() != null) {
            return node.getAddress().getAddress();
        }
        return null;
    }

    /**
     * Resolves unhealthy nodes and validates the removal operation.
     */
    Set<DiscoveryNode> resolveUnhealthyNodesAndValidate(ClusterState currentState) {
        final Set<DiscoveryNode> nodesToRemove = resolveUnhealthyNodesForRemoval(currentState);

        if (nodesToRemove.isEmpty()) {
            throw new IllegalArgumentException(
                "remove unhealthy nodes request matched no unhealthy nodes. "
                    + "All cluster nodes appear to be in the healthy IP list: "
                    + Arrays.toString(healthyNodeIps)
            );
        }

        // Validate that we're not removing all cluster manager nodes
        final DiscoveryNodes allNodes = currentState.nodes();
        Set<DiscoveryNode> remainingMasterNodes = new HashSet<>();

        for (DiscoveryNode node : allNodes) {
            if (node.isClusterManagerNode() && !nodesToRemove.contains(node)) {
                remainingMasterNodes.add(node);
            }
        }

        if (remainingMasterNodes.isEmpty()) {
            throw new IllegalArgumentException(
                "remove unhealthy nodes request would remove all cluster manager nodes. "
                    + "At least one cluster manager node must remain in the cluster."
            );
        }

        // Validate minimum cluster size (optional safety check)
        final int totalNodes = allNodes.getSize();
        final int nodesToRemoveCount = nodesToRemove.size();
        final int remainingNodes = totalNodes - nodesToRemoveCount;

        if (remainingNodes < 1) {
            throw new IllegalArgumentException(
                "remove unhealthy nodes request would remove all nodes from the cluster. " + "At least one node must remain."
            );
        }

        return nodesToRemove;
    }

    public String[] getHealthyNodeIps() {
        return healthyNodeIps;
    }

    public TimeValue getTimeout() {
        return timeout;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;

        // Validate IP addresses format (basic validation)
        for (String ip : healthyNodeIps) {
            if (ip == null || ip.trim().isEmpty()) {
                validationException = new ActionRequestValidationException();
                validationException.addValidationError("Healthy node IP cannot be null or empty");
                break;
            }
        }

        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(healthyNodeIps);
        out.writeTimeValue(timeout);
    }

    @Override
    public String toString() {
        return "RemoveUnhealthyNodesRequest{" + "healthyNodeIps=" + Arrays.asList(healthyNodeIps) + ", " + "timeout=" + timeout + '}';
    }
}
