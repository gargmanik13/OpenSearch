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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * A request to become candidate if current master is unhealthy
 *
 * @opensearch.internal
 */
public class BecomeCandidateRequest extends ClusterManagerNodeRequest<BecomeCandidateRequest> {

    private final String[] healthyNodeNames;
    private final String[] healthyNodeIps;
    private final boolean useNodeNames; // true if using names, false if using IPs
    private final TimeValue timeout;

    /**
     * Construct a request using node names
     */
    public BecomeCandidateRequest(String[] healthyNodeNames, TimeValue timeout) {
        if (healthyNodeNames == null || healthyNodeNames.length == 0) {
            throw new IllegalArgumentException("At least one healthy node name must be specified");
        }

        if (timeout != null && timeout.compareTo(TimeValue.ZERO) < 0) {
            throw new IllegalArgumentException("timeout [" + timeout + "] must be non-negative");
        }

        this.healthyNodeNames = healthyNodeNames;
        this.healthyNodeIps = new String[0];
        this.useNodeNames = true;
        this.timeout = timeout != null ? timeout : TimeValue.timeValueSeconds(30);
    }

    /**
     * Construct a request using node IPs
     */
    public BecomeCandidateRequest(String[] healthyNodeIps, TimeValue timeout, boolean isIpMode) {
        if (!isIpMode) {
            throw new IllegalArgumentException("Use the other constructor for node names");
        }

        if (healthyNodeIps == null || healthyNodeIps.length == 0) {
            throw new IllegalArgumentException("At least one healthy node IP must be specified");
        }

        if (timeout != null && timeout.compareTo(TimeValue.ZERO) < 0) {
            throw new IllegalArgumentException("timeout [" + timeout + "] must be non-negative");
        }

        this.healthyNodeNames = new String[0];
        this.healthyNodeIps = healthyNodeIps;
        this.useNodeNames = false;
        this.timeout = timeout != null ? timeout : TimeValue.timeValueSeconds(30);
    }

    public BecomeCandidateRequest(StreamInput in) throws IOException {
        super(in);
        healthyNodeNames = in.readStringArray();
        healthyNodeIps = in.readStringArray();
        useNodeNames = in.readBoolean();
        timeout = in.readTimeValue();
    }

    /**
     * Check if current master is unhealthy based on the healthy nodes list
     */
    public boolean shouldBecomeCandidateBasedOnMasterHealth(ClusterState currentState) {
        DiscoveryNode currentMaster = currentState.nodes().getClusterManagerNode();

        // If there's no current master, we should become candidate
        if (currentMaster == null) {
            return true;
        }

        if (useNodeNames) {
            // Check by node name
            Set<String> healthyNodeNameSet = new HashSet<>(Arrays.asList(healthyNodeNames));
            return !healthyNodeNameSet.contains(currentMaster.getName());
        } else {
            // Check by node IP
            Set<String> healthyNodeIpSet = new HashSet<>(Arrays.asList(healthyNodeIps));
            String masterIp = extractIpFromNode(currentMaster);
            return masterIp == null || !healthyNodeIpSet.contains(masterIp);
        }
    }

    /**
     * Extracts IP address from a DiscoveryNode.
     */
    private String extractIpFromNode(DiscoveryNode node) {
        if (node.getAddress() != null) {
            return node.getAddress().getAddress();
        }
        return null;
    }

    /**
     * Get information about the current master health status
     */
    public MasterHealthInfo getMasterHealthInfo(ClusterState currentState) {
        DiscoveryNode currentMaster = currentState.nodes().getClusterManagerNode();

        if (currentMaster == null) {
            return new MasterHealthInfo(null, null, null, false, "No active cluster manager found");
        }

        boolean isHealthy;
        String masterIp = extractIpFromNode(currentMaster);

        if (useNodeNames) {
            Set<String> healthyNodeNameSet = new HashSet<>(Arrays.asList(healthyNodeNames));
            isHealthy = healthyNodeNameSet.contains(currentMaster.getName());
        } else {
            Set<String> healthyNodeIpSet = new HashSet<>(Arrays.asList(healthyNodeIps));
            isHealthy = masterIp != null && healthyNodeIpSet.contains(masterIp);
        }

        String status = isHealthy ?
            "Current cluster manager is healthy" :
            "Current cluster manager is not in healthy nodes list";

        return new MasterHealthInfo(currentMaster.getId(), currentMaster.getName(), masterIp, isHealthy, status);
    }

    public String[] getHealthyNodeNames() {
        return healthyNodeNames;
    }

    public String[] getHealthyNodeIps() {
        return healthyNodeIps;
    }

    public boolean isUseNodeNames() {
        return useNodeNames;
    }

    public TimeValue getTimeout() {
        return timeout;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;

        if (useNodeNames) {
            // Validate node names format
            for (String name : healthyNodeNames) {
                if (name == null || name.trim().isEmpty()) {
                    validationException = new ActionRequestValidationException();
                    validationException.addValidationError("Healthy node name cannot be null or empty");
                    break;
                }
            }
        } else {
            // Validate node IPs format
            for (String ip : healthyNodeIps) {
                if (ip == null || ip.trim().isEmpty()) {
                    validationException = new ActionRequestValidationException();
                    validationException.addValidationError("Healthy node IP cannot be null or empty");
                    break;
                }
            }
        }

        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(healthyNodeNames);
        out.writeStringArray(healthyNodeIps);
        out.writeBoolean(useNodeNames);
        out.writeTimeValue(timeout);
    }

    @Override
    public String toString() {
        if (useNodeNames) {
            return "BecomeCandidateRequest{" +
                "healthyNodeNames=" + Arrays.asList(healthyNodeNames) + ", " +
                "timeout=" + timeout +
                '}';
        } else {
            return "BecomeCandidateRequest{" +
                "healthyNodeIps=" + Arrays.asList(healthyNodeIps) + ", " +
                "timeout=" + timeout +
                '}';
        }
    }

    /**
     * Helper class to hold master health information
     */
    public static class MasterHealthInfo {
        private final String masterId;
        private final String masterName;
        private final String masterIp;
        private final boolean isHealthy;
        private final String status;

        public MasterHealthInfo(String masterId, String masterName, String masterIp, boolean isHealthy, String status) {
            this.masterId = masterId;
            this.masterName = masterName;
            this.masterIp = masterIp;
            this.isHealthy = isHealthy;
            this.status = status;
        }

        public String getMasterId() { return masterId; }
        public String getMasterName() { return masterName; }
        public String getMasterIp() { return masterIp; }
        public boolean isHealthy() { return isHealthy; }
        public String getStatus() { return status; }
    }
}
