/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.configuration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.coordination.Coordinator;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.discovery.Discovery;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for becoming candidate if current master is unhealthy
 */
public class TransportBecomeCandidateAction extends TransportClusterManagerNodeAction<
    BecomeCandidateRequest,
    BecomeCandidateResponse> {

    private static final Logger logger = LogManager.getLogger(TransportBecomeCandidateAction.class);

    private final Discovery discovery;

    @Inject
    public TransportBecomeCandidateAction(
        Settings settings,
        ClusterSettings clusterSettings,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Discovery discovery
    ) {
        super(
            BecomeCandidateAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            BecomeCandidateRequest::new,
            indexNameExpressionResolver
        );
        this.discovery = discovery;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected BecomeCandidateResponse read(StreamInput in) throws IOException {
        return new BecomeCandidateResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        BecomeCandidateRequest request,
        ClusterState state,
        ActionListener<BecomeCandidateResponse> listener
    ) throws Exception {

        final DiscoveryNode localNode = clusterService.localNode();
        final String nodeId = localNode.getId();
        final String nodeIp = localNode.getAddress() != null ? localNode.getAddress().getAddress() : "unknown";

        if (request.isUseNodeNames()) {
            logger.info("Received become candidate request for node: {} ({}), healthy node names: [{}]",
                localNode.getName(), nodeId, String.join(", ", request.getHealthyNodeNames()));
        } else {
            logger.info("Received become candidate request for node: {} ({}), healthy node IPs: [{}]",
                localNode.getName(), nodeId, String.join(", ", request.getHealthyNodeIps()));
        }

        try {
            // Get master health information
            BecomeCandidateRequest.MasterHealthInfo masterInfo = request.getMasterHealthInfo(state);

            logger.info("Current master status - ID: {}, Name: {}, IP: {}, Healthy: {}, Status: {}",
                masterInfo.getMasterId(), masterInfo.getMasterName(), masterInfo.getMasterIp(),
                masterInfo.isHealthy(), masterInfo.getStatus());

            // Check if we should become candidate based on master health
            boolean shouldBecomeCandidate = request.shouldBecomeCandidateBasedOnMasterHealth(state);

            if (!shouldBecomeCandidate) {
                logger.info("Current cluster manager is healthy, no need to become candidate for node: {} ({})",
                    localNode.getName(), nodeId);

                listener.onResponse(new BecomeCandidateResponse(
                    nodeId, nodeIp,
                    masterInfo.getMasterId(), masterInfo.getMasterIp(), true
                ));
                return;
            }

            // Check if discovery is Coordinator
            if (!(discovery instanceof Coordinator)) {
                logger.error("Discovery implementation is not a Coordinator, cannot become candidate for node: {} ({})",
                    localNode.getName(), nodeId);

                listener.onFailure(new IllegalStateException(
                    "Discovery implementation is not a Coordinator, cannot become candidate"));
                return;
            }

            // All checks passed - trigger candidate mode
            Coordinator coordinator = (Coordinator) discovery;
            logger.info("Triggering becomeCandidate for node: {} ({}) due to unhealthy master",
                localNode.getName(), nodeId);

            coordinator.becomeCandidate("master_unhealthy");

            logger.info("Successfully triggered candidate mode for node: {} ({})", localNode.getName(), nodeId);

            listener.onResponse(new BecomeCandidateResponse(
                nodeId, nodeIp,
                masterInfo.getMasterId(), masterInfo.getMasterIp(), false
            ));

        } catch (Exception e) {
            logger.error("Error while processing become candidate request for node: {} ({})",
                localNode.getName(), nodeId, e);
            listener.onFailure(new OpenSearchException("Failed to become candidate: " + e.getMessage(), e));
        }
    }

    @Override
    protected ClusterBlockException checkBlock(BecomeCandidateRequest request, ClusterState state) {
        // Allow this operation even when cluster is blocked for recovery scenarios
        return null;
    }

    @Override
    protected boolean localExecute(BecomeCandidateRequest request) {
        // Execute locally on each node to check their view of master health
        return true;
    }
}
