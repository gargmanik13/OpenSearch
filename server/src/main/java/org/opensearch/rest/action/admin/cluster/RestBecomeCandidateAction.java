/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.configuration.BecomeCandidateAction;
import org.opensearch.action.admin.cluster.configuration.BecomeCandidateRequest;
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
 * REST action to become candidate if current master is unhealthy
 */
public class RestBecomeCandidateAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(RestBecomeCandidateAction.class);
    private static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueSeconds(30L);

    @Override
    public String getName() {
        return "become_candidate_action";
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/_cluster/become_candidate"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        BecomeCandidateRequest becomeCandidateRequest = resolveBecomeCandidateRequest(request);
        return channel -> client.execute(
            BecomeCandidateAction.INSTANCE,
            becomeCandidateRequest,
            new RestToXContentListener<>(channel)
        );
    }

    BecomeCandidateRequest resolveBecomeCandidateRequest(final RestRequest request) {
        String healthyNodeNames = request.param("healthy_node_names");
        String healthyNodeIps = request.param("healthy_node_ips");

        // Validate that only one parameter is provided
        boolean hasNames = !Strings.isNullOrEmpty(healthyNodeNames);
        boolean hasIps = !Strings.isNullOrEmpty(healthyNodeIps);

        if (!hasNames && !hasIps) {
            throw new IllegalArgumentException("Either healthy_node_names or healthy_node_ips parameter must be provided");
        }

        if (hasNames && hasIps) {
            throw new IllegalArgumentException("Only one of healthy_node_names or healthy_node_ips can be provided, not both");
        }

        TimeValue timeout = TimeValue.parseTimeValue(
            request.param("timeout"),
            DEFAULT_TIMEOUT,
            getClass().getSimpleName() + ".timeout"
        );

        if (hasNames) {
            String[] healthyNameArray = Strings.splitStringByCommaToArray(healthyNodeNames);
            if (healthyNameArray.length == 0) {
                throw new IllegalArgumentException("At least one healthy node name must be specified");
            }

            logger.debug("Creating become candidate request with healthy node names: [{}], timeout: [{}]",
                String.join(", ", healthyNameArray), timeout);

            return new BecomeCandidateRequest(healthyNameArray, timeout);
        } else {
            String[] healthyIpArray = Strings.splitStringByCommaToArray(healthyNodeIps);
            if (healthyIpArray.length == 0) {
                throw new IllegalArgumentException("At least one healthy node IP must be specified");
            }

            logger.debug("Creating become candidate request with healthy node IPs: [{}], timeout: [{}]",
                String.join(", ", healthyIpArray), timeout);

            return new BecomeCandidateRequest(healthyIpArray, timeout, true);
        }
    }
}
