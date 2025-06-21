/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.configuration;

import org.opensearch.action.ActionType;

/**
 * Action for removing unhealthy nodes from the cluster.
 *
 * @opensearch.internal
 */
public class RemoveUnhealthyNodesAction extends ActionType<RemoveUnhealthyNodesResponse> {

    public static final RemoveUnhealthyNodesAction INSTANCE = new RemoveUnhealthyNodesAction();
    public static final String NAME = "cluster:admin/configuration/remove_unhealthy_nodes";

    private RemoveUnhealthyNodesAction() {
        super(NAME, RemoveUnhealthyNodesResponse::new);
    }
}
