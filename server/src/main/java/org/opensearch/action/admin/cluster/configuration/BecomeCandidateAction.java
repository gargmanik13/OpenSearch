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
 * Action for triggering a node to become a candidate for cluster manager election
 *
 * @opensearch.internal
 */
public class BecomeCandidateAction extends ActionType<BecomeCandidateResponse> {

    public static final BecomeCandidateAction INSTANCE = new BecomeCandidateAction();
    public static final String NAME = "cluster:admin/configuration/become_candidate";

    private BecomeCandidateAction() {
        super(NAME, BecomeCandidateResponse::new);
    }
}
