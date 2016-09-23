package org.apache.mesos.offer.constrain;

import org.apache.mesos.Protos.Offer;

/**
 * A no-op rule which accepts all resources for cases where no placement constraints are applicable.
 */
public class NoopRule implements PlacementRule {

    @Override
    public Offer filter(Offer offer) {
        return offer;
    }
}
