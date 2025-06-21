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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.rest.action.admin.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.configuration.RemoveUnhealthyNodesAction;
import org.opensearch.action.admin.cluster.configuration.RemoveUnhealthyNodesRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * REST action to remove unhealthy nodes from the cluster.
 *
 * @opensearch.api
 */
public class RestRemoveUnhealthyNodesAction extends BaseRestHandler {
    private static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueSeconds(30L);
    private static final Logger logger = LogManager.getLogger(RestRemoveUnhealthyNodesAction.class);

    @Override
    public String getName() {
        return "remove_unhealthy_nodes_action";
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/_cluster/remove_unhealthy_nodes"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        RemoveUnhealthyNodesRequest removeUnhealthyNodesRequest = resolveRemoveUnhealthyNodesRequest(request);
        return channel -> client.execute(
            RemoveUnhealthyNodesAction.INSTANCE,
            removeUnhealthyNodesRequest,
            new RestToXContentListener<>(channel)
        );
    }

    /**
     * Resolves the RemoveUnhealthyNodesRequest from the REST request parameters
     *
     * @param request The REST request
     * @return RemoveUnhealthyNodesRequest with parsed parameters
     */
    RemoveUnhealthyNodesRequest resolveRemoveUnhealthyNodesRequest(final RestRequest request) {
        String healthyNodeIps = request.param("healthy_node_ips");

        if (Strings.isNullOrEmpty(healthyNodeIps)) {
            throw new IllegalArgumentException("healthy_node_ips parameter is required");
        }

        String[] healthyIpArray = Strings.splitStringByCommaToArray(healthyNodeIps);

        // Validate that we have at least one IP
        if (healthyIpArray.length == 0) {
            throw new IllegalArgumentException("At least one healthy node IP must be specified");
        }

        // Parse timeout with default
        TimeValue timeout = TimeValue.parseTimeValue(request.param("timeout"), DEFAULT_TIMEOUT, getClass().getSimpleName() + ".timeout");

        logger.debug("Removing unhealthy nodes, keeping healthy IPs: [{}], timeout: [{}]", String.join(", ", healthyIpArray), timeout);

        return new RemoveUnhealthyNodesRequest(healthyIpArray, timeout);
    }
}
