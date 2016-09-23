package org.apache.mesos.offer.constrain;

import org.apache.mesos.Protos.Offer;

/**
 * Requires that the Offer be from the provided agent ID.
 */
public class AgentRule implements PlacementRule {
    private final String slaveId;

    public AgentRule(String slaveId) {
        this.slaveId = slaveId;
    }

    @Override
    public Offer filter(Offer offer) {
        if (offer.getSlaveId().getValue().equals(slaveId)) {
            return offer;
        } else {
            // agent mismatch: return empty offer
            return offer.toBuilder().clearResources().build();
        }
    }
}
