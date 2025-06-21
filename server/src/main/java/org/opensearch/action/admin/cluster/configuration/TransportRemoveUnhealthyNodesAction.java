/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.admin.cluster.configuration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateTaskConfig;
import org.opensearch.cluster.ClusterStateTaskListener;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.coordination.NodeRemovalClusterStateTaskExecutor;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPool.Names;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Transport action for removing unhealthy nodes from the cluster by comparing
 * with healthy node IPs and removing nodes not in the healthy list.
 *
 * @opensearch.internal
 */
public class TransportRemoveUnhealthyNodesAction extends TransportClusterManagerNodeAction<
    RemoveUnhealthyNodesRequest,
    RemoveUnhealthyNodesResponse> {

    private static final Logger logger = LogManager.getLogger(TransportRemoveUnhealthyNodesAction.class);

    private final AllocationService allocationService;

    @Inject
    public TransportRemoveUnhealthyNodesAction(
        Settings settings,
        ClusterSettings clusterSettings,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        AllocationService allocationService
    ) {
        super(
            RemoveUnhealthyNodesAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            RemoveUnhealthyNodesRequest::new,
            indexNameExpressionResolver
        );
        this.allocationService = allocationService;
    }

    @Override
    protected String executor() {
        return Names.SAME;
    }

    @Override
    protected RemoveUnhealthyNodesResponse read(StreamInput in) throws IOException {
        return new RemoveUnhealthyNodesResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        RemoveUnhealthyNodesRequest request,
        ClusterState state,
        ActionListener<RemoveUnhealthyNodesResponse> listener
    ) throws Exception {

        logger.info("Starting removal of unhealthy nodes based on healthy IPs: {}", String.join(", ", request.getHealthyNodeIps()));

        Set<DiscoveryNode> nodesToRemove;
        try {
            nodesToRemove = request.resolveUnhealthyNodesAndValidate(state);
        } catch (Exception e) {
            logger.warn("Failed to resolve unhealthy nodes: {}", e.getMessage());
            listener.onFailure(e);
            return;
        }

        if (nodesToRemove.isEmpty()) {
            logger.info("No unhealthy nodes found to remove");
            listener.onResponse(new RemoveUnhealthyNodesResponse(List.of()));
            return;
        }

        List<String> nodeIdsToRemove = nodesToRemove.stream().map(DiscoveryNode::getId).collect(Collectors.toList());

        logger.info("Found {} unhealthy nodes to remove: {}", nodesToRemove.size(), nodeIdsToRemove);

        NodeRemovalClusterStateTaskExecutor nodeRemovalExecutor = new NodeRemovalClusterStateTaskExecutor(allocationService, logger);

        // Thread-safe completion tracking
        AtomicInteger totalProcessed = new AtomicInteger(0);
        AtomicBoolean responseAlreadySent = new AtomicBoolean(false);
        List<String> removedNodeIds = Collections.synchronizedList(new ArrayList<>());
        List<Exception> failures = Collections.synchronizedList(new ArrayList<>());

        final int totalNodes = nodesToRemove.size();

        // Submit removal task for each unhealthy node
        for (DiscoveryNode nodeToRemove : nodesToRemove) {
            String nodeIp = nodeToRemove.getAddress() != null ? nodeToRemove.getAddress().getAddress() : "unknown";

            logger.debug("Submitting removal task for unhealthy node: {} with IP: {}", nodeToRemove.getId(), nodeIp);

            clusterService.submitStateUpdateTask(
                "remove-unhealthy-node-" + nodeToRemove.getId(),
                new NodeRemovalClusterStateTaskExecutor.Task(nodeToRemove, "unhealthy.node"),
                ClusterStateTaskConfig.build(Priority.IMMEDIATE),
                nodeRemovalExecutor,
                new ClusterStateTaskListener() {
                    @Override
                    public void onFailure(String source, Exception e) {
                        logger.warn("Failed to remove unhealthy node: {} due to: {}", nodeToRemove.getId(), e.getMessage(), e);

                        failures.add(e);
                        checkAndHandleCompletion();
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        logger.info("Successfully removed unhealthy node: {} with IP: {}", nodeToRemove.getId(), nodeIp);

                        removedNodeIds.add(nodeToRemove.getId());
                        checkAndHandleCompletion();
                    }

                    /**
                     * Thread-safe completion check and handling
                     */
                    private void checkAndHandleCompletion() {
                        if (totalProcessed.incrementAndGet() == totalNodes) {
                            handleCompletion();
                        }
                    }

                    /**
                     * Handle completion of all node removal tasks
                     */
                    private void handleCompletion() {
                        // Ensure we only send response once
                        if (responseAlreadySent.compareAndSet(false, true)) {
                            if (failures.isEmpty()) {
                                // All removals succeeded
                                logger.info("Successfully removed all {} unhealthy nodes: {}", removedNodeIds.size(), removedNodeIds);
                                listener.onResponse(new RemoveUnhealthyNodesResponse(removedNodeIds));

                            } else if (removedNodeIds.isEmpty()) {
                                // All removals failed
                                logger.error("Failed to remove any of the {} unhealthy nodes", totalNodes);
                                Exception primaryException = failures.getFirst();
                                OpenSearchException aggregatedException = new OpenSearchException(
                                    "Failed to remove any unhealthy nodes. First error: " + primaryException.getMessage(),
                                    primaryException
                                );

                                // Add other failures as suppressed exceptions
                                for (int i = 1; i < failures.size(); i++) {
                                    aggregatedException.addSuppressed(failures.get(i));
                                }

                                listener.onFailure(aggregatedException);

                            } else {
                                // Partial success - some nodes removed, some failed
                                logger.warn(
                                    "Partial success: removed {} nodes, failed to remove {} nodes. " + "Successfully removed: {}",
                                    removedNodeIds.size(),
                                    failures.size(),
                                    removedNodeIds
                                );

                                // Return success with the nodes that were actually removed
                                listener.onResponse(new RemoveUnhealthyNodesResponse(removedNodeIds));
                            }
                        }
                    }
                }
            );
        }
    }

    @Override
    protected ClusterBlockException checkBlock(RemoveUnhealthyNodesRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
