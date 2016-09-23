package org.apache.mesos.offer.constrain;

import java.util.HashSet;
import java.util.Set;

import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;

/**
 * Wrapper for another rule which returns the NOT result: Resources which the wrapped rule removed.
 */
public class NotRule implements PlacementRule {

    private final PlacementRule rule;

    public NotRule(PlacementRule rule) {
        this.rule = rule;
    }

    @Override
    public Offer filter(Offer offer) {
        Offer filtered = rule.filter(offer);
        if (filtered.getResourcesCount() == 0) {
            // shortcut: all resources were filtered out, so return all resources
            return offer;
        } else if (filtered.getResourcesCount() == offer.getResourcesCount()) {
            // other shortcut: no resources were filtered out, so return no resources
            return offer.toBuilder().clearResources().build();
        }
        Set<Resource> resourcesToOmit = new HashSet<>();
        for (Resource resource : filtered.getResourcesList()) {
            resourcesToOmit.add(resource);
        }
        Offer.Builder offerBuilder = offer.toBuilder().clearResources();
        for (Resource resource : offer.getResourcesList()) {
            if (!resourcesToOmit.contains(resource)) {
                offerBuilder.addResources(resource);
            }
        }
        return offerBuilder.build();
    }
}
