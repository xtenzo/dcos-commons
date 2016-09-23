package org.apache.mesos.offer.constrain;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;

/**
 * Wrapper for one or more another rules which returns the AND/intersection of those rules.
 */
public class AndRule implements PlacementRule {

    private final Collection<PlacementRule> rules;

    public AndRule(Collection<PlacementRule> rules) {
        this.rules = rules;
    }

    public AndRule(PlacementRule... rules) {
        this.rules = Arrays.asList(rules);
    }

    @Override
    public Offer filter(Offer offer) {
        // Counts of how often each Resource passed each filter:
        Map<Resource, Integer> resourceCounts = new HashMap<>();
        for (PlacementRule rule : rules) {
            Offer filtered = rule.filter(offer);
            for (Resource resource : filtered.getResourcesList()) {
                Integer val = resourceCounts.get(resource);
                if (val == null) {
                    val = 0;
                }
                val++;
                resourceCounts.put(resource, val);
            }
        }
        if (resourceCounts.size() == 0) {
            // shortcut: all resources were filtered out, so return no resources
            return offer.toBuilder().clearResources().build();
        }
        // preserve original ordering (and any original duplicates): test the original list in order
        Offer.Builder offerBuilder = offer.toBuilder().clearResources();
        for (Resource resource : offer.getResourcesList()) {
            Integer val = resourceCounts.get(resource);
            if (val != null && val == rules.size()) {
                offerBuilder.addResources(resource);
            }
        }
        return offerBuilder.build();
    }
}
